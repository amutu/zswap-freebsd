# zswap.ko - compressed swap cache for FreeBSD

A zswap-like compressed swap cache, implemented as a self-contained loadable
kernel module.  No kernel patches: `kldload` / `kldunload`.

```
swap_pager -> /dev/zswap0 (this module) -+- > zstd -> LRU pool  (fast path)
                                         +- > pool full   -> zstd block on disk
                                              incompressible -> raw page on disk
                                              -> backing block device (GEOM)
```

FreeBSD has no `frontswap` layer, so instead of sitting *between* the swap
pager and the disk, the module *is* the swap device and manages the disk layer
itself.  The backing device is reached through a GEOM consumer (the same path
`swapongeom()` uses), never through VFS: calling into a filesystem from the
page allocator path invites re-entrancy.

Backing I/O follows `swapgeom_strategy()`: child bios are submitted with
`g_io_request()` and completed asynchronously; the incoming swap bio completes
only after the last child finishes.  `d_strategy()` therefore never blocks on
the backing device: if a child bio cannot be submitted (allocation failure
under extreme pressure, or the backing device is being reconfigured), the
swap bio fails with `ENOMEM` and the pager retries the page later, instead
of sleeping on backing I/O in the page-out path.  Cold pages leave the pool
through an LRU shrinker thread, which can afford to block because it is a
separate kproc.

A spilled page is stored compressed whenever it compresses: the slot is still
4 KB of address space, but only the compressed length reaches the device.  The
I/O length is rounded up to the backing sector size, so the saving is real on
a 512-byte-sector device and zero on native 4Kn.  Reads of a compressed block
cannot be decompressed where the bio completes - `bio_done` may run inside a
critical section and zstd allocates its workspace - so they are handed to a
taskqueue thread which decompresses and then completes the swap bio.

## Files

| file | purpose |
|---|---|
| `zswap.c` | swap device front end, LRU pool, shrinker, sysctls, stats |
| `zswap_zstd.c` | zstd glue with `ZSTD_customMem` -> kernel malloc |
| `zswap.h` | internal interface |
| `kernel_compat.h` | freestanding shims (`NULL`, `ptrdiff_t`) force-included |

zstd sources come from the kernel-safe copy already in the base system
(`sys/contrib/openzfs/module/zstd`), so there is no external dependency and no
libc inside the module.

## Build

Requires a FreeBSD source tree at `/usr/src` matching the running kernel.

```sh
make                # -> zswap.ko
sudo make install   # -> /boot/modules/zswap.ko
```

## Use

```sh
sudo kldload zswap
sudo sysctl vfs.zswap.backing=da1          # may be much smaller than the device
sudo sysctl vfs.zswap.backing_selftest=1   # 0 = write/read round trip OK
sudo swapon /dev/zswap0
```

The backing device is validated when it is configured.  A device that cannot
hold even one page, one whose sector size does not divide `PAGE_SIZE`, or an
unknown provider is rejected immediately with `EINVAL`, instead of failing
later on every spilled page.  A device smaller than `vfs.zswap.mediasize` is
*not* an error - capacity is decoupled (see *Sizing*) - it only logs which
part of the swap area has disk backing.  See *Sizing* below for how to pick
both numbers.

## Sizing: mediasize vs backing device vs RAM pool

These three numbers are independent:

| number | what it bounds | default |
|---|---|---|
| `vfs.zswap.mediasize` | logical swap capacity reported to the kernel | `min(physmem/2, 16 GB)` |
| the backing device | disk space for pages that could not stay compressed | required |
| `vfs.zswap.max_pool_percent` | compressed pages held in RAM | 20% of physmem |

A page is stored one of two ways, decided at write time:

- **resident** - compressed into the RAM pool;
- **spilled** - written to a backing slot allocated on demand (one 4 KB slot
  each, tracked by a bitmap).

Backing slots are not derived from the page index, so the disk no longer has
to be as large as the device.  Slot bookkeeping is a two-level bitmap (1 bit
per slot plus 1 bit per 64 slots), so finding a free slot costs one word test
and a `ctz` in the common case, and a full scan costs `nslots/4096` word tests
instead of `nslots` bit tests.  Capacity comes from compression:

```
effective swap ~= (pool_limit + backing_size) / avg_compressed_page
                  * PAGE_SIZE          (incompressible pages fall back to 1 slot
                                        each, so the backing is a hard floor)
```

Measured on this 4 GB VM, device 2031 MB, **backing 256 MB**, pool 25%:

```
swapped out 281,702 pages (1.1 GB)
   196,878 pages (770 MB) stayed compressed in RAM
    65,536 pages (256 MB) went to disk   -> backing exactly full
backing_full = 564   (page-outs that found no slot; pager retried them)
decompression errors 0, I/O errors 0, all 1,049,600 pages verified
```

The same 1.1 GB of swap needed a 1.1 GB disk before.  With
`kenv zswap.mediasize_mb=6144` the device reports **6 GB** of swap on a
3.97 GB machine while still using the 256 MB disk.

Planning rules:

- Set `mediasize` at least as large as the capacity you intend to serve. It is
  the ceiling the kernel sees; once it is exhausted, swap fails outright.
- Size the backing device for the *incompressible fraction* of the workload,
  not for the whole swap area.  Too small and `backing_full` climbs: the
  page-out bio returns ENOSPC and the pager retries the page later.  No data
  is lost, but the machine starts thrashing instead of swapping smoothly.
- `mediasize` is fixed at device creation: set `zswap.mediasize_mb` before
  `kldload`, there is no runtime knob.
- `swapinfo` reports `mediasize`, not the disk size.  A 6 GB device on a
  256 MB backing is a legitimate, working configuration.

## Tunables

| sysctl | default | meaning |
|---|---|---|
| `vfs.zswap.enable` | 1 | 0 = pass through, no compression |
| `vfs.zswap.compression_level` | 3 | zstd level, 1-22 |
| `vfs.zswap.max_pool_percent` | 20 | hard cap on pool size, % of physmem |
| `vfs.zswap.shrinker_enable` | 1 | run the LRU eviction thread |
| `vfs.zswap.shrinker_threshold` | 80 | pool % that wakes the shrinker |
| `vfs.zswap.shrinker_target` | 60 | pool % the shrinker drains down to |
| `vfs.zswap.threads` | ncpu | concurrent compression contexts, 1-32, runtime settable |
| `vfs.zswap.backing` | - | backing provider, `da1` or `/dev/da1` |

Counters live under `vfs.zswap.stats.*`; write anything to
`vfs.zswap.stats.reset` to zero them.  Notable ones: `submit_fail` counts
bios that failed because a child bio could not be submitted (see above),
and `busy_fail` counts writes deferred because a shrinker writeback of the
same page was in flight (the pager retries both).  `spill_bytes` is the
payload actually moved to and from the backing device, so
`spill_bytes / (2 * spill_write)` is the average stored page size: 4096 means
nothing was compressed (4Kn backing, or incompressible data).

Unload refuses with `EBUSY` while anything holds the device, so always
`swapoff /dev/zswap0` first.

## Architectures

Cross-compiles for amd64, armv7 and aarch64 against a FreeBSD 15 source tree
(the build needs an explicit compiler target):

```sh
make TARGET=amd64 TARGET_ARCH=amd64 MACHINE=amd64 MACHINE_ARCH=amd64 \
     CC="cc --target=x86_64-unknown-freebsd15.2"
make TARGET=arm TARGET_ARCH=armv7 MACHINE=arm MACHINE_ARCH=armv7 \
     CC="cc --target=armv7-unknown-freebsd15.2"
```

Only aarch64 has been boot-tested; amd64/armv7 are compile-verified only.
The code is word-size clean (32-bit arithmetic is done in `uint64_t`), page
size only matters as "the block size", and byte order is irrelevant.  The real
coupling is the FreeBSD version: the module uses 15.x kernel interfaces
(`disk_alloc`, `swapongeom`, `kthread_exit()`).

## Known limitations

Compression runs on a pool of zstd contexts (`vfs.zswap.threads`), outside the
pool lock; decompression stays under the pool lock because it is cheap.  On a
single-domain system the win is shorter lock hold, not higher page-out
throughput, since one pagedaemon thread issues the writes.  No dump support,
no per-cgroup control, no same-page dedup, and no pure-zram mode (the device
refuses to open without a backing device).  The shrinker evicts at roughly the
speed of the backing device, so under a sustained burst the pool stays near its
cap and drains to `shrinker_target` once pressure subsides - and every evicted
page consumes a backing slot, so a long burst eventually reaches `backing_full`
rather than freeing space.  A spilled page still occupies a hash entry (~64 B),
so a fully spilled device costs RAM proportional to its page count.

See `REPORT.md` for the full list and the measured numbers.
