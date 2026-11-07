/*-
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * zswap - a zswap(4)-like compressed swap cache for FreeBSD.
 *
 * FreeBSD has no frontswap layer, so a loadable module cannot insert itself
 * between swap_pager and the swap device.  Instead this module *is* a swap
 * device: the kernel swaps pages to /dev/zswap0, we compress them into a RAM
 * pool, and pages that do not fit are written verbatim to a backing disk.
 *
 * Backing I/O follows swap_pager's own GEOM client (swapongeom() /
 * swapgeom_strategy()):
 *   - consumer attached to the provider, G_CF_DIRECT_SEND/RECEIVE,
 *   - child bios submitted with g_io_request() and completed asynchronously,
 *   - the incoming bio is completed only after the last child bio is done,
 *     so d_strategy() never blocks on the backing device.
 *
 * Pool pages are evicted by an LRU shrinker thread (kproc) with configurable
 * trigger and target thresholds.
 *
 * No kernel patches; kldload/kldunload only.
 */

#include <sys/param.h>
#include <sys/systm.h>
#include <sys/kernel.h>
#include <sys/module.h>
#include <sys/malloc.h>
#include <sys/mutex.h>
#include <sys/sysctl.h>
#include <sys/counter.h>
#include <sys/bio.h>
#include <sys/disk.h>
#include <sys/conf.h>
#include <sys/proc.h>
#include <sys/kthread.h>
#include <sys/sema.h>
#include <sys/smp.h>
#include <sys/sx.h>
#include <sys/uio.h>
#include <sys/queue.h>
#include <sys/libkern.h>
#include <sys/taskqueue.h>
#include <sys/limits.h>

#include <geom/geom.h>
#include <geom/geom_disk.h>

#include "zswap.h"

MALLOC_DEFINE(M_ZSWAP, "zswap", "zswap compressed page pool");

#define	ZSWAP_HASH_BITS		17
#define	ZSWAP_HASH_SIZE		(1u << ZSWAP_HASH_BITS)
#define	ZSWAP_LEVEL_MIN		1
#define	ZSWAP_LEVEL_MAX		22
#define	ZSWAP_POOL_PCT_MIN	1
#define	ZSWAP_POOL_PCT_MAX	90
#define	ZSWAP_SHR_PCT_MIN	1
#define	ZSWAP_SHR_PCT_MAX	100
#define	ZSWAP_THREADS_MAX	32
#define	ZSWAP_MEDIASIZE_MIN	(64ULL << 20)
#define	ZSWAP_MEDIASIZE_MAX	(16ULL << 30)

/* One page in the pool. */
struct zswap_ent {
	uint64_t	ze_idx;		/* page index into the device */
	uint64_t	ze_boff;	/* backing slot, valid when ze_spilled */
	uint32_t	ze_len;		/* stored length (compressed or raw) */
	uint32_t	ze_seq;		/* bumped on replace: stale eviction guard */
	u_int		ze_ref;		/* table ref + eviction refs */
	u_int		ze_busy;	/* eviction in progress */
	u_int		ze_raw;		/* stored uncompressed */
	u_int		ze_spilled;	/* lives on the backing device */
	struct zswap_ent *ze_next;	/* hash chain */
	void		*ze_data;
	TAILQ_ENTRY(zswap_ent) ze_lru;
};

/*
 * Per-parent-bio async I/O context.  zo_pending starts at 1 ("hold") and is
 * released at the end of zswap_strategy(); the parent bio completes when
 * every child bio has also been released.
 */
struct zswap_worker {
	struct mtx	zw_mtx;
	zswap_zctx	*zw_ctx;
};

/*
 * Per-child-bio context.  A compressed page needs a buffer that outlives the
 * bio, and on the read side a post-processing step (decompress) that may
 * sleep, so each such bio carries one of these to the completion callback.
 */
struct zswap_child {
	void		*zc_buf;	/* buffer owned by this bio */
	void		*zc_dst;	/* decompress target; NULL for writes */
	uint32_t	 zc_len;	/* stored (compressed) bytes */
	uint64_t	 zc_boff;	/* backing slot this bio touches, 0 = none */
	struct zswap_ent *zc_ent;	/* write: entry kept busy until done */
};

/* Deferred read of a busy spilled page; runs on zswap_tq. */
struct zswap_rtask {
	struct task	 rt_task;
	struct zswap_op *rt_op;
	uint64_t	 rt_idx;
	void		*rt_dst;
};

/* Deferred decompression of one spilled page; runs on zswap_tq. */
struct zswap_dtask {
	struct task	 dt_task;
	struct zswap_op	*dt_op;
	void		*dt_buf;
	void		*dt_dst;
	uint32_t	 dt_len;
	int		 dt_error;
};

struct zswap_op {
	struct bio	*zo_parent;
	int		 zo_error;
	u_int		 zo_pending;
};

/* Synchronous waiter: shrinker thread and allocation fallback. */
struct zswap_sync {
	struct sema	zs_sem;
	int		zs_error;
};

static struct zswap_ent	**zswap_tab;
static TAILQ_HEAD(, zswap_ent) zswap_lru = TAILQ_HEAD_INITIALIZER(zswap_lru);
static u_long		 zswap_nent;
static size_t		 zswap_pool_bytes;
static size_t		 zswap_pool_limit;
static struct mtx	 zswap_mtx;
static struct disk	*zswap_dp;
static off_t		 zswap_mediasize;
static int		 zswap_refs;
static int		 zswap_berr_reported;
static struct zswap_worker *zswap_workers;
static int		 zswap_nworkers;
static struct sx	 zswap_wlock;
static u_int		 zswap_wrr;
static u_int		 zswap_inflight;
static zswap_zctx	*zswap_rdctx;
static struct mtx	 zswap_rd_mtx;		/* guards zswap_rdctx */
static struct taskqueue *zswap_tq;		/* deferred decompression */
static u_int		 zswap_tasks;		/* tasks queued or running */
static size_t		 zswap_bsize = 512;	/* backing sector size */

/*
 * Backing slots.  A page no longer maps to a fixed backing offset: slots are
 * allocated on demand, so the device can be much larger than the disk behind
 * it - compressed pages never touch the disk at all.  One bit per PAGE_SIZE.
 */
static uint64_t	*zswap_bm;	/* level 1: one bit per slot, 1 = used */
static uint64_t	*zswap_bm_sum;	/* level 2: one bit per bm word, 1 = has free */
static uint64_t	 zswap_bm_nwords;
static uint64_t	 zswap_bm_nsum;
static uint64_t	 zswap_nslots;
static uint64_t	 zswap_nfree;
static uint64_t	 zswap_bm_hint;	/* summary word hint */

/*
 * Slot lifetime guards.  A backing slot may not be returned to the bitmap
 * while a child bio that reads or writes it is still in flight: the slot
 * could be reallocated to a different page and the stale I/O would corrupt
 * it.  Every child bio that targets a slot takes a reservation on it (under
 * the pool mutex, before the bio is submitted); freeing a slot with live
 * reservations only marks it pending, and the last unreservation - the
 * child completion - returns it to the bitmap.
 */
struct zswap_guard {
	uint64_t	gd_boff;
	u_int		gd_cnt;
	int		gd_pending;
	LIST_ENTRY(zswap_guard) gd_link;
};
#define	ZSWAP_NGUARD	64
static LIST_HEAD(, zswap_guard) zswap_guards[ZSWAP_NGUARD];

static void	zswap_slot_free_locked(uint64_t);

static struct zswap_guard *
zswap_guard_find_locked(uint64_t boff)
{
	struct zswap_guard *g;

	LIST_FOREACH(g, &zswap_guards[boff & (ZSWAP_NGUARD - 1)], gd_link)
		if (g->gd_boff == boff)
			return (g);
	return (NULL);
}

/* Record that a child bio targeting boff is about to be submitted. */
static int
zswap_slot_reserve_locked(uint64_t boff)
{
	struct zswap_guard *g;

	g = zswap_guard_find_locked(boff);
	if (g == NULL) {
		g = malloc(sizeof(*g), M_ZSWAP, M_NOWAIT | M_ZERO);
		if (g == NULL)
			return (ENOMEM);
		g->gd_boff = boff;
		LIST_INSERT_HEAD(&zswap_guards[boff & (ZSWAP_NGUARD - 1)],
		    g, gd_link);
	}
	g->gd_cnt++;
	return (0);
}

/* Child bio done: the slot is only handed back with its last user. */
static void
zswap_slot_unreserve_locked(uint64_t boff)
{
	struct zswap_guard *g;

	g = zswap_guard_find_locked(boff);
	if (g == NULL)
		return;
	if (--g->gd_cnt != 0)
		return;
	if (g->gd_pending != 0)
		zswap_slot_free_locked(boff);
	LIST_REMOVE(g, gd_link);
	free(g, M_ZSWAP);
}

/*
 * Give up a slot's ownership (entry replaced or dropped).  If I/O is still
 * in flight to it, defer the actual free to the last unreservation.
 */
static void
zswap_slot_release_locked(uint64_t boff)
{
	struct zswap_guard *g;

	g = zswap_guard_find_locked(boff);
	if (g != NULL && g->gd_cnt > 0) {
		g->gd_pending = 1;
		return;
	}
	zswap_slot_free_locked(boff);
}

/*
 * Configuration/lifecycle lock.  Shared: backing consumer use (I/O submit,
 * d_open).  Exclusive: backing (re)configuration, selftest, unload.  Also
 * serializes the zswap_refs/zswap_dirty checks against d_open, so a backing
 * swap or the offset-0 selftest can never race a swapon.
 */
static struct sx	 zswap_cfg_lock;
static int		 zswap_closing;	/* unload started: fail new opens */
static volatile u_int	 zswap_children;	/* async child bios in flight */
static int		 zswap_semas_inited;
static volatile int	 zswap_dirty;	/* pages have been stored/spilled */

static int	zswap_enable = 1;
static int	zswap_level = 3;
static int	zswap_pool_percent = 20;
static int	zswap_shrinker_enable = 1;
static int	zswap_shrinker_threshold = 80;	/* % of pool_limit */
static int	zswap_shrinker_target = 60;	/* % of pool_limit */
static size_t	zswap_shr_trigger;
static size_t	zswap_shr_target;

static struct proc *zswap_shr_proc;
static struct sema  zswap_shr_wake;
static struct sema  zswap_shr_done;
static int	    zswap_shr_running;
static int	    zswap_shr_pending;
static int	    zswap_shutdown;
static char	   *zswap_shr_buf;

static counter_u64_t	zswap_c_pages_out;
static counter_u64_t	zswap_c_pages_in;
static counter_u64_t	zswap_c_stored;
static counter_u64_t	zswap_c_evicted;
static counter_u64_t	zswap_c_compr_ok;
static counter_u64_t	zswap_c_compr_fail;
static counter_u64_t	zswap_c_not_worth;
static counter_u64_t	zswap_c_passthru;
static counter_u64_t	zswap_c_spill_write;
static counter_u64_t	zswap_c_spill_read;
static counter_u64_t	zswap_c_ram_read;
static counter_u64_t	zswap_c_decompr_fail;
static counter_u64_t	zswap_c_backing_err;
static counter_u64_t	zswap_c_bytes_raw;
static counter_u64_t	zswap_c_bytes_compr;
static counter_u64_t	zswap_c_shr_evicted;
static counter_u64_t	zswap_c_shr_err;
static counter_u64_t	zswap_c_submit_fail;
static counter_u64_t	zswap_c_keep_raw;
static counter_u64_t	zswap_c_busy_fail;
static counter_u64_t	zswap_c_backing_full;	/* out of backing slots */
static counter_u64_t	zswap_c_missing;		/* read for an unknown page */
static counter_u64_t	zswap_c_slot_scans;	/* level-2 words walked */
static counter_u64_t	zswap_c_spill_bytes;	/* payload bytes moved to disk */
static counter_u64_t	zswap_c_compress_waits;

/* ------------------------------------------------------------------ */
/* compressed pool; helpers below require zswap_mtx                   */
/* ------------------------------------------------------------------ */

/*
 * Slots are tracked by a two level bitmap: level 1 has one bit per slot,
 * level 2 has one bit per level-1 word and is set when that word still has a
 * free bit.  Allocation therefore walks level 2 (64x fewer words) and then
 * finds the free bit with ctz, instead of testing one bit at a time.
 * All helpers require zswap_mtx.
 */
static int64_t
zswap_slot_alloc_locked(void)
{
	uint64_t i, s, w, bit, freebits, steps;

	if (zswap_nfree == 0)
		return (-1);
	steps = 0;
	for (i = 0; i < zswap_bm_nsum; i++) {
		s = (zswap_bm_hint + i) % zswap_bm_nsum;
		steps++;
		if (zswap_bm_sum[s] == 0)
			continue;
		counter_u64_add(zswap_c_slot_scans, steps);
		w = s * 64 + (uint64_t)__builtin_ctzll(zswap_bm_sum[s]);
		freebits = ~zswap_bm[w];
		bit = (uint64_t)__builtin_ctzll(freebits);
		zswap_bm[w] |= 1ull << bit;
		if (zswap_bm[w] == ~0ull)
			zswap_bm_sum[s] &= ~(1ull << (w - s * 64));
		zswap_nfree--;
		zswap_bm_hint = s;
		return ((int64_t)(w * 64 + bit));
	}
	return (-1);
}

static void
zswap_slot_free_locked(uint64_t s)
{
	uint64_t w, s2;

	KASSERT(s < zswap_nslots, ("zswap slot out of range"));
	w = s >> 6;
	if ((zswap_bm[w] & (1ull << (s & 63))) == 0)
		return;
	if (zswap_bm[w] == ~0ull) {
		s2 = w >> 6;
		zswap_bm_sum[s2] |= 1ull << (w & 63);
	}
	zswap_bm[w] &= ~(1ull << (s & 63));
	zswap_nfree++;
}

static struct zswap_ent **
zswap_slot(uint64_t idx)
{
	struct zswap_ent **pp;

	pp = &zswap_tab[idx & (ZSWAP_HASH_SIZE - 1)];
	while (*pp != NULL && (*pp)->ze_idx != idx)
		pp = &(*pp)->ze_next;
	return (pp);
}

static void
zswap_ent_release_locked(struct zswap_ent *e)
{

	KASSERT(e->ze_ref > 0, ("zswap entry ref underflow"));
	if (--e->ze_ref != 0)
		return;
	free(e->ze_data, M_ZSWAP);
	free(e, M_ZSWAP);
}

static void
zswap_drop_locked(uint64_t idx)
{
	struct zswap_ent **pp;
	struct zswap_ent *e;

	pp = zswap_slot(idx);
	e = *pp;
	if (e == NULL)
		return;
	*pp = e->ze_next;
	if (e->ze_spilled != 0) {
		zswap_slot_release_locked(e->ze_boff);
	} else {
		TAILQ_REMOVE(&zswap_lru, e, ze_lru);
		KASSERT(zswap_pool_bytes >= e->ze_len + sizeof(*e),
		    ("zswap pool accounting"));
		zswap_pool_bytes -= e->ze_len + sizeof(*e);
		counter_u64_add(zswap_c_evicted, 1);
	}
	zswap_nent--;
	zswap_ent_release_locked(e);
}

/*
 * Insert or replace the page at idx.  Takes ownership of data.  Returns
 * ENOMEM when the table entry itself cannot be allocated.
 */
/*
 * Insert or replace a page.  Two shapes:
 *   spilled == 0: compressed (or raw) data in RAM, charged to the pool;
 *   spilled == 1: the page lives in backing slot boff, data is NULL.
 * Takes ownership of data.  Returns ENOMEM if the entry cannot be allocated.
 */
static int
zswap_put_locked(uint64_t idx, void *data, uint32_t len, int raw,
    uint64_t boff, int spilled)
{
	struct zswap_ent **pp;
	struct zswap_ent *e;

	pp = zswap_slot(idx);
	e = *pp;
	if (e != NULL) {
		if (e->ze_spilled != 0) {
			zswap_slot_release_locked(e->ze_boff);
		} else {
			KASSERT(zswap_pool_bytes >= e->ze_len + sizeof(*e),
			    ("zswap pool accounting"));
			zswap_pool_bytes -= e->ze_len + sizeof(*e);
			free(e->ze_data, M_ZSWAP);
			TAILQ_REMOVE(&zswap_lru, e, ze_lru);
		}
		e->ze_data = data;
		e->ze_len = len;
		e->ze_raw = raw;
		e->ze_boff = boff;
		e->ze_spilled = spilled;
		e->ze_seq++;
		if (spilled == 0) {
			zswap_pool_bytes += len + sizeof(*e);
			TAILQ_INSERT_TAIL(&zswap_lru, e, ze_lru);
		}
		return (0);
	}
	e = malloc(sizeof(*e), M_ZSWAP, M_NOWAIT);
	if (e == NULL)
		return (ENOMEM);
	e->ze_idx = idx;
	e->ze_data = data;
	e->ze_len = len;
	e->ze_raw = raw;
	e->ze_boff = boff;
	e->ze_spilled = spilled;
	e->ze_seq = 1;
	e->ze_ref = 1;
	e->ze_busy = 0;
	e->ze_next = NULL;
	*pp = e;
	zswap_nent++;
	if (spilled == 0) {
		TAILQ_INSERT_TAIL(&zswap_lru, e, ze_lru);
		zswap_pool_bytes += len + sizeof(*e);
	}
	return (0);
}

/* ------------------------------------------------------------------ */
/* compression workers                                                */
/*                                                                    */
/* Compression is the only expensive step, so it runs on a pool of     */
/* contexts: several bios can compress at the same time and none of    */
/* them hold the pool mutex while doing it.  Decompression is cheap    */
/* and always runs under the pool mutex, which is also its only lock   */
/* (zswap_rdctx).                                                      */
/* ------------------------------------------------------------------ */

static void
zswap_workers_free(void)
{
	struct zswap_worker *old;
	int i, n;

	sx_xlock(&zswap_wlock);
	old = zswap_workers;
	n = zswap_nworkers;
	zswap_workers = NULL;
	zswap_nworkers = 0;
	sx_xunlock(&zswap_wlock);

	for (i = 0; i < n; i++) {
		mtx_destroy(&old[i].zw_mtx);
		zswap_zstd_destroy(old[i].zw_ctx);
	}
	free(old, M_ZSWAP);
}

static int
zswap_workers_resize(int n)
{
	struct zswap_worker *new, *old;
	int i, oldn;

	new = malloc(sizeof(*new) * n, M_ZSWAP, M_WAITOK | M_ZERO);
	for (i = 0; i < n; i++) {
		new[i].zw_ctx = zswap_zstd_create();
		if (new[i].zw_ctx == NULL) {
			while (i-- > 0)
				zswap_zstd_destroy(new[i].zw_ctx);
			free(new, M_ZSWAP);
			return (ENOMEM);
		}
		mtx_init(&new[i].zw_mtx, "zswapwk", NULL, MTX_DEF);
	}

	sx_xlock(&zswap_wlock);
	old = zswap_workers;
	oldn = zswap_nworkers;
	zswap_workers = new;
	zswap_nworkers = n;
	sx_xunlock(&zswap_wlock);

	for (i = 0; i < oldn; i++) {
		mtx_destroy(&old[i].zw_mtx);
		zswap_zstd_destroy(old[i].zw_ctx);
	}
	free(old, M_ZSWAP);
	return (0);
}

/* Returns a worker with its mutex held; caller must hold zswap_wlock shared. */
static struct zswap_worker *
zswap_worker_acquire(void)
{
	struct zswap_worker *w;
	u_int start, i, idx, n;

	n = zswap_nworkers;
	start = atomic_fetchadd_int(&zswap_wrr, 1);
	for (i = 0; i < n; i++) {
		idx = (start + i) % n;
		w = &zswap_workers[idx];
		/*
		 * mtx_trylock() returns non-zero on success in this FreeBSD
		 * version; testing for 0 took a failed trylock as ownership.
		 */
		if (mtx_trylock(&w->zw_mtx) != 0)
			goto got;
	}
	/* All busy: wait for our round-robin slot. */
	counter_u64_add(zswap_c_compress_waits, 1);
	w = &zswap_workers[start % n];
	mtx_lock(&w->zw_mtx);
got:
	atomic_add_int(&zswap_inflight, 1);
	return (w);
}

static void
zswap_worker_release(struct zswap_worker *w)
{

	atomic_subtract_int(&zswap_inflight, 1);
	mtx_unlock(&w->zw_mtx);
}

/* ------------------------------------------------------------------ */
/* backing store: a GEOM consumer on a raw provider                   */
/* ------------------------------------------------------------------ */

static struct g_class zswap_backing_class = {
	.name =		"ZSWAP_BACKING",
	.version =	G_VERSION,
};
static struct g_geom	*zswap_bg_geom;
static struct g_consumer *zswap_bg_cp;
static char		 zswap_backing_path[MAXPATHLEN];

/*
 * Drop one reference on the parent bio's operation, and complete that bio once
 * the last one is gone.  Reached from the child bio callback (pages that are
 * stored verbatim) and from the decompression task (compressed pages).
 */
static void
zswap_op_done(struct zswap_op *op, int error)
{
	struct bio *parent;

	if (error != 0)
		atomic_cmpset_int(&op->zo_error, 0, error);
	parent = op->zo_parent;
	if (atomic_fetchadd_int(&op->zo_pending, -1) != 1)
		return;

	/* Last reference: complete the swap bio we are acting for. */
	if (op->zo_error != 0) {
		parent->bio_error = op->zo_error;
		if (atomic_cmpset_int(&zswap_berr_reported, 0, 1))
			printf("zswap: backing I/O error %d\n", op->zo_error);
	}
	parent->bio_resid = 0;
	free(op, M_ZSWAP);
	biodone(parent);
}

/*
 * Deferred decompression of one spilled page.  Runs on zswap_tq, so this is a
 * sleepable context: zstd may allocate its workspace here.  Doing it in the
 * bio callback (or in d_strategy) is what previously deadlocked.
 */
static void
zswap_dtask_fn(void *arg, int pending __unused)
{
	struct zswap_dtask *dt;
	size_t ulen;
	int error;

	dt = arg;
	error = dt->dt_error;
	if (error == 0) {
		ulen = 0;
		mtx_lock(&zswap_rd_mtx);
		if (zswap_zstd_decompress(zswap_rdctx, dt->dt_dst,
		    ZSWAP_BLOCK_SIZE, dt->dt_buf, dt->dt_len, &ulen) != 0 ||
		    ulen != ZSWAP_BLOCK_SIZE) {
			counter_u64_add(zswap_c_decompr_fail, 1);
			error = EIO;
		}
		mtx_unlock(&zswap_rd_mtx);
	}
	free(dt->dt_buf, M_ZSWAP);
	zswap_op_done(dt->dt_op, error);
	free(dt, M_ZSWAP);
	atomic_subtract_int(&zswap_tasks, 1);
}

static void
zswap_child_done(struct bio *bp)
{
	struct zswap_op *op;
	struct zswap_child *zc;
	struct zswap_dtask *dt;
	uint64_t boff;
	int error;

	op = bp->bio_caller1;
	zc = bp->bio_caller2;
	boff = (zc != NULL) ? zc->zc_boff : 0;
	error = bp->bio_error;
	g_destroy_bio(bp);
	atomic_subtract_int(&zswap_children, 1);
	if (error != 0)
		counter_u64_add(zswap_c_backing_err, 1);

	/*
	 * The bio that used this slot is finished: drop its reservation,
	 * returning the slot to the bitmap if it was released in the
	 * meantime (entry replaced or dropped while the I/O was in flight).
	 * A finished write also clears its hold on the entry, waking any
	 * reader that deferred because the page had not landed yet.
	 */
	if (boff != 0 || (zc != NULL && zc->zc_ent != NULL)) {
		mtx_lock(&zswap_mtx);
		if (boff != 0)
			zswap_slot_unreserve_locked(boff);
		if (zc != NULL && zc->zc_ent != NULL) {
			zc->zc_ent->ze_busy--;
			wakeup(zc->zc_ent);
			zswap_ent_release_locked(zc->zc_ent);
		}
		mtx_unlock(&zswap_mtx);
	}

	if (zc == NULL) {
		zswap_op_done(op, error);
		return;
	}
	if (zc->zc_dst != NULL && error == 0) {
		dt = malloc(sizeof(*dt), M_ZSWAP, M_NOWAIT | M_ZERO);
		if (dt != NULL) {
			TASK_INIT(&dt->dt_task, 0, zswap_dtask_fn, dt);
			dt->dt_op = op;
			dt->dt_buf = zc->zc_buf;
			dt->dt_dst = zc->zc_dst;
			dt->dt_len = zc->zc_len;
			free(zc, M_ZSWAP);
			atomic_add_int(&zswap_tasks, 1);
			taskqueue_enqueue(zswap_tq, &dt->dt_task);
			return;
		}
		error = ENOMEM;
	}
	free(zc->zc_buf, M_ZSWAP);
	free(zc, M_ZSWAP);
	zswap_op_done(op, error);
}

static void
zswap_sync_done(struct bio *bp)
{
	struct zswap_sync *ws;

	ws = bp->bio_caller1;
	ws->zs_error = bp->bio_error;
	g_destroy_bio(bp);
	sema_post(&ws->zs_sem);
}

static void
zswap_bio_init(struct bio *bp, int cmd, void *buf, off_t off, size_t len)
{

	bp->bio_cmd = cmd;
	bp->bio_offset = off;
	bp->bio_length = len;
	bp->bio_data = buf;
}

/*
 * Asynchronous: the parent bio is completed by zswap_child_done().
 *
 * Called from the swap pager's strategy path with VM locks held, so it
 * must not sleep on a backing (re)configuration: the shared config lock is
 * only tried, and if an exclusive user (set_backing/selftest/unload) has
 * it, the bio simply fails and the pager retries.  Exclusive users never
 * wait on the VM, so this cannot deadlock the page-out path.
 */
static int
zswap_io_submit(struct zswap_op *op, int cmd, void *buf, off_t off, size_t len,
    struct zswap_child *zc, uint64_t boff)
{
	struct bio *bp;

	bp = g_new_bio();
	if (bp == NULL)
		return (ENOMEM);
	if (sx_try_slock(&zswap_cfg_lock) == 0) {
		g_destroy_bio(bp);
		return (ENOMEM);
	}
	if (zswap_bg_cp == NULL) {
		sx_sunlock(&zswap_cfg_lock);
		g_destroy_bio(bp);
		return (ENXIO);
	}
	zswap_bio_init(bp, cmd, buf, off, len);
	bp->bio_caller1 = op;
	bp->bio_caller2 = zc;
	if (zc != NULL)
		zc->zc_boff = boff;	/* slot guard key */
	bp->bio_done = zswap_child_done;
	atomic_add_int(&op->zo_pending, 1);
	atomic_add_int(&zswap_children, 1);
	g_io_request(bp, zswap_bg_cp);
	sx_sunlock(&zswap_cfg_lock);
	return (0);
}

/*
 * Blocking; only for threads that may sleep (shrinker, selftest).
 * The caller holds zswap_cfg_lock (shared or exclusive), which pins the
 * backing consumer for the duration of the I/O.
 */
static int
zswap_io_sync_locked(int cmd, void *buf, off_t off, size_t len)
{
	struct zswap_sync ws;
	struct bio *bp;
	int error;

	if (zswap_bg_cp == NULL)
		return (ENXIO);
	bp = g_new_bio();
	if (bp == NULL)
		return (ENOMEM);
	sema_init(&ws.zs_sem, 0, "zswapsync");
	ws.zs_error = 0;
	zswap_bio_init(bp, cmd, buf, off, len);
	bp->bio_caller1 = &ws;
	bp->bio_done = zswap_sync_done;
	g_io_request(bp, zswap_bg_cp);
	sema_wait(&ws.zs_sem);
	error = ws.zs_error;
	sema_destroy(&ws.zs_sem);
	return (error);
}

/* Caller holds zswap_cfg_lock exclusively. */
static void
zswap_backing_close_locked(void)
{

	if (zswap_bg_cp == NULL)
		return;
	g_topology_lock();
	g_access(zswap_bg_cp, -1, -1, 0);
	g_detach(zswap_bg_cp);
	g_destroy_consumer(zswap_bg_cp);
	g_destroy_geom(zswap_bg_geom);
	g_topology_unlock();
	zswap_bg_cp = NULL;
	zswap_bg_geom = NULL;
	zswap_backing_path[0] = '\0';

	mtx_lock(&zswap_mtx);
	free(zswap_bm, M_ZSWAP);
	free(zswap_bm_sum, M_ZSWAP);
	zswap_bm = NULL;
	zswap_bm_sum = NULL;
	zswap_bm_nwords = 0;
	zswap_bm_nsum = 0;
	zswap_nslots = 0;
	zswap_nfree = 0;
	zswap_bm_hint = 0;
	mtx_unlock(&zswap_mtx);
}

/*
 * Attach a consumer to the named provider.  Capacity and sector size are
 * validated here, at configuration time, so a too-small backing device is
 * rejected up front instead of turning into I/O errors at run time.
 */
static int
zswap_set_backing(const char *path)
{
	struct g_provider *pp;
	const char *name;
	uint64_t nslots, nwords, i;
	uint64_t *bm, *bm_sum;
	int error;

	sx_xlock(&zswap_cfg_lock);
	if (zswap_closing != 0) {
		sx_xunlock(&zswap_cfg_lock);
		return (ENXIO);
	}
	if (zswap_refs > 0) {
		sx_xunlock(&zswap_cfg_lock);
		return (EBUSY);
	}
	if (atomic_load_acq_int(&zswap_dirty) != 0) {
		printf("zswap: refusing to repoint backing, pages already "
		    "stored on %s; swapoff and unload first\n",
		    zswap_backing_path);
		sx_xunlock(&zswap_cfg_lock);
		return (EBUSY);
	}
	name = path;
	if (strncmp(name, "/dev/", 5) == 0)
		name += 5;
	if (name[0] == '\0') {
		sx_xunlock(&zswap_cfg_lock);
		return (EINVAL);
	}
	if (strcmp(name, zswap_backing_path) == 0 && zswap_bg_cp != NULL) {
		sx_xunlock(&zswap_cfg_lock);
		return (0);
	}

	if (zswap_bg_cp != NULL)
		zswap_backing_close_locked();

	g_topology_lock();
	pp = g_provider_by_name(name);
	if (pp == NULL) {
		g_topology_unlock();
		sx_xunlock(&zswap_cfg_lock);
		return (ENXIO);
	}
	/*
	 * The device no longer maps page N to offset N * PAGE_SIZE: backing
	 * slots are allocated on demand, so the disk only has to hold the pages
	 * that did not fit compressed in RAM.  It may therefore be much
	 * smaller than the device.  Too small and incompressible pages will
	 * eventually fail to find a slot (counted in backing_full).
	 */
	if (pp->mediasize < ZSWAP_BLOCK_SIZE) {
		printf("zswap: %s is too small to hold a page\n", name);
		g_topology_unlock();
		sx_xunlock(&zswap_cfg_lock);
		return (EINVAL);
	}
	if (pp->mediasize < zswap_mediasize)
		printf("zswap: backing %s is %jdMB, smaller than the %jdMB "
		    "device; only spilled pages need it\n", name,
		    (intmax_t)(pp->mediasize >> 20),
		    (intmax_t)(zswap_mediasize >> 20));
	if (pp->sectorsize == 0 || pp->sectorsize > ZSWAP_BLOCK_SIZE ||
	    (ZSWAP_BLOCK_SIZE % pp->sectorsize) != 0) {
		printf("zswap: %s sector size %u does not divide %d\n", name,
		    pp->sectorsize, ZSWAP_BLOCK_SIZE);
		g_topology_unlock();
		sx_xunlock(&zswap_cfg_lock);
		return (EINVAL);
	}

	/* geom rejects I/O whose length is not a multiple of this. */
	zswap_bsize = pp->sectorsize;

	zswap_bg_geom = g_new_geomf(&zswap_backing_class, "zswap_backing");
	zswap_bg_cp = g_new_consumer(zswap_bg_geom);
	zswap_bg_cp->index = 1;		/* active, as swapongeom() does */
	zswap_bg_cp->flags |= G_CF_DIRECT_SEND | G_CF_DIRECT_RECEIVE;
	error = g_attach(zswap_bg_cp, pp);
	if (error == 0)
		error = g_access(zswap_bg_cp, 1, 1, 0);
	if (error != 0) {
		if (zswap_bg_cp->provider != NULL)
			g_detach(zswap_bg_cp);
		g_destroy_consumer(zswap_bg_cp);
		g_destroy_geom(zswap_bg_geom);
		zswap_bg_cp = NULL;
		zswap_bg_geom = NULL;
		g_topology_unlock();
		sx_xunlock(&zswap_cfg_lock);
		return (error);
	}
	g_topology_unlock();

	strlcpy(zswap_backing_path, name, sizeof(zswap_backing_path));
	atomic_store_rel_int(&zswap_berr_reported, 0);

	mtx_lock(&zswap_mtx);
	nslots = (uint64_t)pp->mediasize / ZSWAP_BLOCK_SIZE;
	nwords = (nslots + 63) / 64;
	bm = malloc(sizeof(*bm) * nwords, M_ZSWAP, M_NOWAIT | M_ZERO);
	bm_sum = malloc(sizeof(*bm_sum) * ((nwords + 63) / 64),
	    M_ZSWAP, M_NOWAIT | M_ZERO);
	if (bm == NULL || bm_sum == NULL) {
		mtx_unlock(&zswap_mtx);
		free(bm, M_ZSWAP);
		free(bm_sum, M_ZSWAP);
		g_topology_lock();
		g_access(zswap_bg_cp, -1, -1, 0);
		g_detach(zswap_bg_cp);
		g_destroy_consumer(zswap_bg_cp);
		g_destroy_geom(zswap_bg_geom);
		g_topology_unlock();
		zswap_bg_cp = NULL;
		zswap_bg_geom = NULL;
		zswap_backing_path[0] = '\0';
		sx_xunlock(&zswap_cfg_lock);
		printf("zswap: no memory for a %ju-slot bitmap\n",
		    (uintmax_t)nslots);
		return (ENOMEM);
	}
	/* Bits past the end are marked used so they are never handed out. */
	if ((nslots & 63) != 0)
		bm[nwords - 1] = ~((1ull << (nslots & 63)) - 1);
	for (i = 0; i < nwords; i++)
		bm_sum[i >> 6] |= 1ull << (i & 63);
	zswap_bm = bm;
	zswap_bm_sum = bm_sum;
	zswap_bm_nwords = nwords;
	zswap_bm_nsum = (nwords + 63) / 64;
	zswap_nslots = nslots;
	zswap_nfree = nslots;
	zswap_bm_hint = 0;
	mtx_unlock(&zswap_mtx);
	printf("zswap: backing %s, %jdMB, sector %u\n", name,
	    (intmax_t)(pp->mediasize >> 20), pp->sectorsize);
	sx_xunlock(&zswap_cfg_lock);
	return (0);
}

/* ------------------------------------------------------------------ */
/* LRU shrinker                                                       */
/* ------------------------------------------------------------------ */

/*
 * Evict the least recently used resident page: decompress, write the raw
 * page to the backing device, then drop it from the pool.  Runs in its own
 * thread, so blocking on the backing device is acceptable here.
 *
 * The entry stays linked and readable while the write is in flight, and it
 * holds an extra reference.  Commit re-checks the generation, so a page that
 * was rewritten during writeback is never discarded.
 */
static int
zswap_evict_one(void)
{
	struct zswap_ent *e, **pp;
	uint32_t seq;
	size_t blen;
	int error;
	int64_t slot;

	mtx_lock(&zswap_mtx);
	TAILQ_FOREACH(e, &zswap_lru, ze_lru) {
		if (e->ze_busy == 0)
			break;
	}
	if (e == NULL) {
		mtx_unlock(&zswap_mtx);
		return (ENOENT);
	}
	/*
	 * Copy out what is already compressed: no decompress on this path, so
	 * the eviction writes exactly the bytes that live in the pool.
	 */
	blen = e->ze_len;
	memcpy(zswap_shr_buf, e->ze_data, blen);
	slot = zswap_slot_alloc_locked();
	if (slot < 0) {
		counter_u64_add(zswap_c_backing_full, 1);
		mtx_unlock(&zswap_mtx);
		return (ENOSPC);
	}
	e->ze_busy = 1;
	e->ze_ref++;
	seq = e->ze_seq;
	mtx_unlock(&zswap_mtx);

	sx_slock(&zswap_cfg_lock);
	error = zswap_io_sync_locked(BIO_WRITE, zswap_shr_buf,
	    (off_t)slot * ZSWAP_BLOCK_SIZE, roundup2(blen, zswap_bsize));
	sx_sunlock(&zswap_cfg_lock);

	mtx_lock(&zswap_mtx);
	e->ze_busy = 0;
	wakeup(e);
	if (error == 0) {
		pp = zswap_slot(e->ze_idx);
		if (*pp == e && e->ze_seq == seq) {
			/* Resident -> spilled, without losing the page. */
			KASSERT(zswap_pool_bytes >= e->ze_len + sizeof(*e),
			    ("zswap pool accounting"));
			zswap_pool_bytes -= e->ze_len + sizeof(*e);
			free(e->ze_data, M_ZSWAP);
			e->ze_data = NULL;
			/* ze_len / ze_raw describe what is now on disk. */
			e->ze_boff = (uint64_t)slot;
			e->ze_spilled = 1;
			TAILQ_REMOVE(&zswap_lru, e, ze_lru);
			counter_u64_add(zswap_c_spill_bytes, blen);
			counter_u64_add(zswap_c_shr_evicted, 1);
		} else {
			zswap_slot_free_locked((uint64_t)slot);
		}
	} else {
		zswap_slot_free_locked((uint64_t)slot);
		counter_u64_add(zswap_c_shr_err, 1);
	}
	zswap_ent_release_locked(e);
	mtx_unlock(&zswap_mtx);

	return (error);
}

static void
zswap_shrinker(void *arg)
{
	int error;

	for (;;) {
		sema_wait(&zswap_shr_wake);
		if (zswap_shutdown != 0)
			break;
		zswap_shr_pending = 0;
		if (zswap_shrinker_enable == 0)
			continue;
		for (;;) {
			mtx_lock(&zswap_mtx);
			if (zswap_shutdown != 0 ||
			    zswap_pool_bytes <= zswap_shr_target) {
				mtx_unlock(&zswap_mtx);
				break;
			}
			mtx_unlock(&zswap_mtx);
			error = zswap_evict_one();
			if (error != 0)
				break;
		}
		if (zswap_shutdown != 0)
			break;
	}
	sema_post(&zswap_shr_done);
	kthread_exit();
}

static void
zswap_shrinker_kick(void)
{

	if (zswap_shrinker_enable == 0 || zswap_shutdown != 0)
		return;
	if (zswap_shr_pending != 0)
		return;
	zswap_shr_pending = 1;
	sema_post(&zswap_shr_wake);
}

/* ------------------------------------------------------------------ */
/* I/O path                                                           */
/* ------------------------------------------------------------------ */

/* Finish a swap bio: now, or when the last child bio is done. */
static void
zswap_complete(struct bio *bp, int error)
{
	struct zswap_op *op;

	op = bp->bio_driver1;
	bp->bio_driver1 = NULL;
	if (op != NULL) {
		if (error != 0)
			atomic_cmpset_int(&op->zo_error, 0, error);
		/* Release the hold; completes when children are done too. */
		if (atomic_fetchadd_int(&op->zo_pending, -1) != 1)
			return;
		if (op->zo_error != 0)
			bp->bio_error = op->zo_error;
		bp->bio_resid = 0;
		free(op, M_ZSWAP);
		biodone(bp);
		return;
	}
	bp->bio_resid = 0;
	if (error != 0)
		bp->bio_error = error;
	biodone(bp);
}

static struct zswap_op *
zswap_op_get(struct bio *bp)
{
	struct zswap_op *op;

	op = bp->bio_driver1;
	if (op != NULL)
		return (op);
	op = malloc(sizeof(*op), M_ZSWAP, M_NOWAIT | M_ZERO);
	if (op == NULL)
		return (NULL);
	op->zo_parent = bp;
	op->zo_pending = 1;		/* hold until zswap_complete() */
	bp->bio_driver1 = op;
	return (op);
}

/*
 * Send one page to the backing device.  Always asynchronous: this runs in
 * the page-out path, which must never block on backing I/O.  If a child
 * bio cannot be submitted (allocation failure, or the backing consumer is
 * being reconfigured), the swap bio fails and the pager retries the page
 * later, instead of risking a pageout deadlock under memory pressure.
 */
static int
zswap_backing_page(struct bio *bp, int rw, void *data, uint64_t boff,
    struct zswap_ent *ent)
{
	struct zswap_op *op;
	struct zswap_child *zc;

	/* A slot-touching bio always carries a zc: it holds the guard key. */
	zc = malloc(sizeof(*zc), M_ZSWAP, M_NOWAIT | M_ZERO);
	if (zc == NULL) {
		counter_u64_add(zswap_c_submit_fail, 1);
		return (ENOMEM);
	}
	zc->zc_ent = ent;	/* writes keep the entry busy until done */
	op = zswap_op_get(bp);
	if (op == NULL || zswap_io_submit(op,
	    (rw == UIO_READ) ? BIO_READ : BIO_WRITE, data,
	    (off_t)boff * ZSWAP_BLOCK_SIZE, ZSWAP_BLOCK_SIZE, zc,
	    boff) != 0) {
		free(zc, M_ZSWAP);
		counter_u64_add(zswap_c_submit_fail, 1);
		return (ENOMEM);
	}
	return (0);
}

/*
 * Submit one page-sized child bio that carries its own buffer and, for reads,
 * the decompression target.  Ownership of zc (and of zc->zc_buf) passes to the
 * completion callback.
 */
static int
zswap_backing_page_buf(struct bio *bp, int rw, void *buf, uint64_t boff,
    size_t len, struct zswap_child *zc)
{
	struct zswap_op *op;

	op = zswap_op_get(bp);
	if (op == NULL) {
		free(zc->zc_buf, M_ZSWAP);
		free(zc, M_ZSWAP);
		counter_u64_add(zswap_c_submit_fail, 1);
		return (ENOMEM);
	}
	if (zswap_io_submit(op, (rw == UIO_READ) ? BIO_READ : BIO_WRITE,
	    buf, (off_t)boff * ZSWAP_BLOCK_SIZE, len, zc, boff) != 0) {
		free(zc->zc_buf, M_ZSWAP);
		free(zc, M_ZSWAP);
		counter_u64_add(zswap_c_submit_fail, 1);
		return (ENOMEM);
	}
	return (0);
}

static int
zswap_write_pages(struct bio *bp)
{
	off_t off;
	size_t left;
	char *data;
	void *scratch;
	int error, spill, compress_ok, raw;
	size_t clen, blen, iolen;
	uint64_t idx;
	int64_t slot;
	void *cbuf;
	struct zswap_child *zc;
	struct zswap_ent *e, *e2;
	struct zswap_worker *w;

	off = bp->bio_offset;
	left = bp->bio_bcount;
	data = bp->bio_data;

	if (off % ZSWAP_BLOCK_SIZE != 0 || left % ZSWAP_BLOCK_SIZE != 0)
		return (EINVAL);

	scratch = malloc(ZSWAP_BLOCK_SIZE, M_ZSWAP, M_NOWAIT);
	error = 0;

	w = NULL;
	if (zswap_enable != 0 && scratch != NULL) {
		sx_slock(&zswap_wlock);
		w = zswap_worker_acquire();
	}

	while (left > 0) {
		idx = (uint64_t)off / ZSWAP_BLOCK_SIZE;
		clen = 0;
		compress_ok = 0;
		if (w != NULL && zswap_zstd_compress(w->zw_ctx, scratch,
		    ZSWAP_BLOCK_SIZE, data, ZSWAP_BLOCK_SIZE, zswap_level,
		    &clen) == 0)
			compress_ok = 1;

		spill = 1;
		slot = -1;
		cbuf = NULL;
		zc = NULL;
		raw = 1;
		blen = ZSWAP_BLOCK_SIZE;
		iolen = ZSWAP_BLOCK_SIZE;
		mtx_lock(&zswap_mtx);
		if (w == NULL) {
			counter_u64_add(zswap_c_passthru, 1);
		} else if (compress_ok != 0) {
			counter_u64_add(zswap_c_bytes_raw, ZSWAP_BLOCK_SIZE);
			counter_u64_add(zswap_c_bytes_compr, clen);
			counter_u64_add(zswap_c_compr_ok, 1);
			if (clen + sizeof(struct zswap_ent) < ZSWAP_BLOCK_SIZE &&
			    zswap_pool_bytes + clen + sizeof(struct zswap_ent) <=
			    zswap_pool_limit) {
				void *store;

				store = malloc(clen, M_ZSWAP, M_NOWAIT);
				if (store != NULL) {
					memcpy(store, scratch, clen);
					if (zswap_put_locked(idx, store, clen, 0,
					    0, 0) == 0) {
						spill = 0;
						counter_u64_add(zswap_c_stored, 1);
					} else {
						free(store, M_ZSWAP);
					}
				}
			} else {
				counter_u64_add(zswap_c_not_worth, 1);
			}
		} else {
			counter_u64_add(zswap_c_compr_fail, 1);
		}

		if (spill != 0) {
			/*
			 * On-demand slot instead of the old page-index offset.
			 * This is what decouples device capacity from the disk,
			 * and it also removes the "two writers, one offset"
			 * race the positional mapping had.
			 *
			 * Spill the compressed form when we have one: the slot
			 * is still 4 KB of address space, but only clen bytes
			 * reach the disk.  Incompressible pages go verbatim.
			 */
			if (compress_ok != 0) {
				blen = clen;
				raw = 0;
			}
			/*
			 * Only a compressed block needs a buffer of its own: a
			 * page that goes out verbatim is read straight from the
			 * incoming bio, exactly as before.  Allocating 4 KB per
			 * page here is what pushed the pass-through and
			 * tiny-pool cases into ENOMEM under real pressure.
			 */
			if (raw == 0) {
				/* geom wants a sector multiple. */
				iolen = roundup2(blen, zswap_bsize);
				cbuf = malloc(iolen, M_ZSWAP, M_NOWAIT);
				if (cbuf == NULL) {
					counter_u64_add(zswap_c_submit_fail,
					    1);
					mtx_unlock(&zswap_mtx);
					error = ENOMEM;
					break;
				}
				memcpy(cbuf, scratch, clen);
				if (iolen > clen)
					memset((char *)cbuf + clen, 0,
					    iolen - clen);
			}
			slot = zswap_slot_alloc_locked();
			if (slot < 0) {
				free(cbuf, M_ZSWAP);
				counter_u64_add(zswap_c_backing_full, 1);
				mtx_unlock(&zswap_mtx);
				error = ENOSPC;
				break;
			}
		if (zswap_put_locked(idx, NULL, blen, raw,
		    (uint64_t)slot, 1) != 0) {
			zswap_slot_free_locked((uint64_t)slot);
			free(cbuf, M_ZSWAP);
			mtx_unlock(&zswap_mtx);
			error = ENOMEM;
			break;
		}
		/*
		 * Reserve the slot before the mapping becomes visible and
		 * before any I/O is submitted: a concurrent replace of this
		 * index must not hand the slot back to the bitmap while our
		 * child bio is in flight.
		 */
		if (zswap_slot_reserve_locked((uint64_t)slot) != 0) {
			zswap_drop_locked(idx);
			free(cbuf, M_ZSWAP);
			mtx_unlock(&zswap_mtx);
			error = ENOMEM;
			break;
		}
		/*
		 * The mapping is visible before the data reaches the disk;
		 * keep the entry busy so a concurrent read defers until the
		 * write child has landed.
		 */
		e = *zswap_slot(idx);
		e->ze_busy++;
		e->ze_ref++;
		if (raw == 0) {
			zc = malloc(sizeof(*zc), M_ZSWAP,
			    M_NOWAIT | M_ZERO);
			if (zc == NULL) {
				zswap_drop_locked(idx);
				zswap_slot_unreserve_locked((uint64_t)slot);
				e->ze_busy--;
				wakeup(e);
				zswap_ent_release_locked(e);
				free(cbuf, M_ZSWAP);
				mtx_unlock(&zswap_mtx);
				error = ENOMEM;
				break;
			}
			zc->zc_buf = cbuf;
			zc->zc_len = blen;
			zc->zc_ent = e;
		}
		}
		if (zswap_pool_bytes >= zswap_shr_trigger)
			zswap_shrinker_kick();
		mtx_unlock(&zswap_mtx);

		if (spill != 0) {
			if (raw != 0)
				error = zswap_backing_page(bp, UIO_WRITE, data,
				    (uint64_t)slot, e);
			else
				error = zswap_backing_page_buf(bp, UIO_WRITE,
				    cbuf, (uint64_t)slot, iolen, zc);
			if (error != 0) {
				/* Nothing usable reached the disk: drop mapping. */
				mtx_lock(&zswap_mtx);
				e2 = *zswap_slot(idx);
				if (e2 != NULL && e2->ze_spilled != 0 &&
				    e2->ze_boff == (uint64_t)slot)
					zswap_drop_locked(idx);
				zswap_slot_unreserve_locked((uint64_t)slot);
				e->ze_busy--;
				wakeup(e);
				zswap_ent_release_locked(e);
				mtx_unlock(&zswap_mtx);
				break;
			}
			counter_u64_add(zswap_c_spill_bytes, blen);
			counter_u64_add(zswap_c_spill_write, 1);
		}

		off += ZSWAP_BLOCK_SIZE;
		data += ZSWAP_BLOCK_SIZE;
		left -= ZSWAP_BLOCK_SIZE;
	}

	if (w != NULL) {
		zswap_worker_release(w);
		sx_sunlock(&zswap_wlock);
	}
	if (scratch != NULL)
		free(scratch, M_ZSWAP);
	return (error);
}

/*
 * Deferred read of one spilled page whose write was still in flight.  Runs
 * on zswap_tq (sleepable): wait for the entry to settle, then finish the
 * read exactly like the synchronous path would have.
 */
static void
zswap_readwait_fn(void *arg, int pending __unused)
{
	struct zswap_rtask *rt;
	struct zswap_op *op;
	struct zswap_child *zc;
	struct zswap_ent *e;
	void *cb;
	uint64_t boff, idx;
	size_t ulen, iolen;
	uint32_t clen;
	int error, raw;

	rt = arg;
	op = rt->rt_op;
	idx = rt->rt_idx;
	error = 0;

	mtx_lock(&zswap_mtx);
	for (;;) {
		e = *zswap_slot(idx);
		if (e == NULL) {
			counter_u64_add(zswap_c_missing, 1);
			error = EIO;
			break;
		}
		if (e->ze_busy == 0)
			break;
		e->ze_ref++;
		msleep(e, &zswap_mtx, PRIBIO, "zswaprwait", 0);
		zswap_ent_release_locked(e);
	}

	if (error == 0 && e->ze_spilled != 0) {
		boff = e->ze_boff;
		clen = e->ze_len;
		raw = e->ze_raw;
		if (zswap_slot_reserve_locked(boff) != 0)
			error = ENOMEM;
		mtx_unlock(&zswap_mtx);
		if (error == 0) {
			zc = malloc(sizeof(*zc), M_ZSWAP, M_NOWAIT | M_ZERO);
			if (raw != 0) {
				if (zc == NULL) {
					error = ENOMEM;
				} else if (zswap_io_submit(op, BIO_READ,
				    rt->rt_dst, (off_t)boff * ZSWAP_BLOCK_SIZE,
				    ZSWAP_BLOCK_SIZE, zc, boff) != 0) {
					free(zc, M_ZSWAP);
					error = ENOMEM;
				}
			} else {
				iolen = roundup2(clen, zswap_bsize);
				cb = malloc(iolen, M_ZSWAP, M_NOWAIT);
				if (cb == NULL || zc == NULL) {
					free(cb, M_ZSWAP);
					free(zc, M_ZSWAP);
					error = ENOMEM;
				} else {
					zc->zc_buf = cb;
					zc->zc_dst = rt->rt_dst;
					zc->zc_len = clen;
					if (zswap_io_submit(op, BIO_READ, cb,
					    (off_t)boff * ZSWAP_BLOCK_SIZE,
					    iolen, zc, boff) != 0) {
						free(cb, M_ZSWAP);
						free(zc, M_ZSWAP);
						error = ENOMEM;
					}
				}
			}
			if (error != 0) {
				mtx_lock(&zswap_mtx);
				zswap_slot_unreserve_locked(boff);
				mtx_unlock(&zswap_mtx);
			} else {
				counter_u64_add(zswap_c_spill_bytes, clen);
				counter_u64_add(zswap_c_spill_read, 1);
			}
		}
	} else if (error == 0) {
		/* Resident page: serve it under the pool mutex, as usual. */
		if (e->ze_raw != 0) {
			memcpy(rt->rt_dst, e->ze_data, ZSWAP_BLOCK_SIZE);
		} else {
			ulen = 0;
			mtx_lock(&zswap_rd_mtx);
			if (zswap_zstd_decompress(zswap_rdctx, rt->rt_dst,
			    ZSWAP_BLOCK_SIZE, e->ze_data, e->ze_len,
			    &ulen) != 0 || ulen != ZSWAP_BLOCK_SIZE) {
				counter_u64_add(zswap_c_decompr_fail, 1);
				error = EIO;
			}
			mtx_unlock(&zswap_rd_mtx);
		}
		if (error == 0) {
			TAILQ_REMOVE(&zswap_lru, e, ze_lru);
			TAILQ_INSERT_TAIL(&zswap_lru, e, ze_lru);
			counter_u64_add(zswap_c_ram_read, 1);
		}
		mtx_unlock(&zswap_mtx);
	} else {
		mtx_unlock(&zswap_mtx);
	}

	zswap_op_done(op, error);
	free(rt, M_ZSWAP);
	atomic_subtract_int(&zswap_tasks, 1);
}

static int
zswap_read_pages(struct bio *bp)
{
	off_t off;
	size_t left;
	char *data;
	int error, got, raw;
	size_t ulen, clen, iolen;
	uint64_t idx, boff;
	void *cb;
	struct zswap_child *zc;
	struct zswap_ent *e;
	struct zswap_op *op;
	struct zswap_rtask *rt;

	off = bp->bio_offset;
	left = bp->bio_bcount;
	data = bp->bio_data;

	if (off % ZSWAP_BLOCK_SIZE != 0 || left % ZSWAP_BLOCK_SIZE != 0)
		return (EINVAL);

	error = 0;
	while (left > 0) {
		idx = (uint64_t)off / ZSWAP_BLOCK_SIZE;
		got = 0;
		boff = 0;
		clen = 0;
		raw = 0;

		mtx_lock(&zswap_mtx);
		e = *zswap_slot(idx);
		if (e == NULL) {
			mtx_unlock(&zswap_mtx);
			counter_u64_add(zswap_c_missing, 1);
			error = EIO;
			break;
		}
		if (e->ze_spilled != 0 && e->ze_busy != 0) {
			/*
			 * The page's write (or an eviction) is still in
			 * flight: the slot may not hold the mapped data yet.
			 * d_strategy cannot sleep, so hand the page to the
			 * taskqueue, wait there for the entry to settle and
			 * finish the read from that thread.
			 */
			op = zswap_op_get(bp);
			rt = malloc(sizeof(*rt), M_ZSWAP, M_NOWAIT | M_ZERO);
			if (op == NULL || rt == NULL) {
				free(rt, M_ZSWAP);
				mtx_unlock(&zswap_mtx);
				error = ENOMEM;
				break;
			}
			TASK_INIT(&rt->rt_task, 0, zswap_readwait_fn, rt);
			rt->rt_op = op;
			rt->rt_idx = idx;
			rt->rt_dst = data;
			atomic_add_int(&op->zo_pending, 1);
			atomic_add_int(&zswap_tasks, 1);
			mtx_unlock(&zswap_mtx);
			taskqueue_enqueue(zswap_tq, &rt->rt_task);
			goto nextpage;
		}
		if (e->ze_spilled != 0) {
			boff = e->ze_boff;
			clen = e->ze_len;
			raw = e->ze_raw;
			/* Pin the slot against replace-driven reuse. */
			if (zswap_slot_reserve_locked(boff) != 0) {
				mtx_unlock(&zswap_mtx);
				error = ENOMEM;
				break;
			}
			mtx_unlock(&zswap_mtx);
			if (raw != 0) {
				error = zswap_backing_page(bp, UIO_READ, data,
				    boff, NULL);
			} else {
				/*
				 * Compressed block: read it into its own buffer
				 * and let the taskqueue thread decompress it.
				 * d_strategy must not sleep, and bio_done may
				 * run inside a critical section, so neither is
				 * allowed to run zstd here.
				 */
				iolen = roundup2(clen, zswap_bsize);
				cb = malloc(iolen, M_ZSWAP, M_NOWAIT);
				zc = malloc(sizeof(*zc), M_ZSWAP,
				    M_NOWAIT | M_ZERO);
				if (cb == NULL || zc == NULL) {
					free(cb, M_ZSWAP);
					free(zc, M_ZSWAP);
					mtx_lock(&zswap_mtx);
					zswap_slot_unreserve_locked(boff);
					mtx_unlock(&zswap_mtx);
					error = ENOMEM;
					break;
				}
				zc->zc_buf = cb;
				zc->zc_dst = data;
				zc->zc_len = clen;
				error = zswap_backing_page_buf(bp, UIO_READ, cb,
				    boff, iolen, zc);
			}
			if (error != 0) {
				mtx_lock(&zswap_mtx);
				zswap_slot_unreserve_locked(boff);
				mtx_unlock(&zswap_mtx);
				break;
			}
			counter_u64_add(zswap_c_spill_bytes, clen);
			counter_u64_add(zswap_c_spill_read, 1);
		} else {
			if (e->ze_raw != 0) {
				memcpy(data, e->ze_data, ZSWAP_BLOCK_SIZE);
				got = 1;
			} else {
				ulen = 0;
				mtx_lock(&zswap_rd_mtx);
				if (zswap_zstd_decompress(zswap_rdctx, data,
				    ZSWAP_BLOCK_SIZE, e->ze_data, e->ze_len,
				    &ulen) == 0 && ulen == ZSWAP_BLOCK_SIZE)
					got = 1;
				else
					counter_u64_add(zswap_c_decompr_fail, 1);
				mtx_unlock(&zswap_rd_mtx);
			}
			if (got != 0) {
				TAILQ_REMOVE(&zswap_lru, e, ze_lru);
				TAILQ_INSERT_TAIL(&zswap_lru, e, ze_lru);
				counter_u64_add(zswap_c_ram_read, 1);
			} else {
				mtx_unlock(&zswap_mtx);
				error = EIO;
				break;
			}
			mtx_unlock(&zswap_mtx);
		}

nextpage:
		off += ZSWAP_BLOCK_SIZE;
		data += ZSWAP_BLOCK_SIZE;
		left -= ZSWAP_BLOCK_SIZE;
	}
	return (error);
}

static int
zswap_delete_pages(struct bio *bp)
{
	off_t off;
	size_t left;
	uint64_t idx;
	struct zswap_ent *e;

	off = bp->bio_offset;
	left = bp->bio_bcount;
	if (off % ZSWAP_BLOCK_SIZE != 0 || left % ZSWAP_BLOCK_SIZE != 0)
		return (EINVAL);

	mtx_lock(&zswap_mtx);
	while (left > 0) {
		idx = (uint64_t)off / ZSWAP_BLOCK_SIZE;
		e = *zswap_slot(idx);
		if (e != NULL && e->ze_busy != 0) {
			/*
			 * A writeback of this page is in flight.  Wait for
			 * it to commit (or fail) before dropping the entry,
			 * so a later read or rewrite of this index cannot
			 * hit the backing offset while the stale writeback
			 * may still land there.  BIO_DELETE is issued from
			 * sleepable context (page-in, swapoff, exit).
			 */
			e->ze_ref++;
			while (e->ze_busy != 0)
				msleep(e, &zswap_mtx, PRIBIO, "zswapdel", 0);
			zswap_drop_locked(idx);
			zswap_ent_release_locked(e);
		} else {
			zswap_drop_locked(idx);
		}
		off += ZSWAP_BLOCK_SIZE;
		left -= ZSWAP_BLOCK_SIZE;
	}
	mtx_unlock(&zswap_mtx);
	return (0);
}

static void
zswap_strategy(struct bio *bp)
{
	struct zswap_op *op;
	int error;

	switch (bp->bio_cmd) {
	case BIO_READ:
		counter_u64_add(zswap_c_pages_in,
		    bp->bio_bcount / ZSWAP_BLOCK_SIZE);
		error = zswap_read_pages(bp);
		break;
	case BIO_WRITE:
		atomic_store_rel_int(&zswap_dirty, 1);
		counter_u64_add(zswap_c_pages_out,
		    bp->bio_bcount / ZSWAP_BLOCK_SIZE);
		error = zswap_write_pages(bp);
		break;
	case BIO_DELETE:
		error = zswap_delete_pages(bp);
		break;
	case BIO_FLUSH:
		/*
		 * Forward the flush to the backing device instead of
		 * pretending it succeeded: spilled pages may sit in the
		 * backing device's write cache.  Fall through to
		 * zswap_complete(), which releases the op hold; the bio
		 * then completes when the child bio finishes.
		 */
		op = zswap_op_get(bp);
		if (op != NULL) {
			error = zswap_io_submit(op, BIO_FLUSH, NULL, 0, 0,
			    NULL, 0);
			if (error != 0)
				counter_u64_add(zswap_c_submit_fail, 1);
		} else {
			counter_u64_add(zswap_c_submit_fail, 1);
			error = ENOMEM;
		}
		break;
	default:
		error = EOPNOTSUPP;
		break;
	}
	zswap_complete(bp, error);
}

/* ------------------------------------------------------------------ */
/* disk front end                                                      */
/* ------------------------------------------------------------------ */

/*
 * Release everything the pool holds.  Called when the last user closes the
 * device (swapoff, or a plain close), because the swap pager keeps no state
 * for us: after swapoff every entry refers to a swap block nobody will ask
 * for again, and the slots must go back to the bitmap or they leak for the
 * lifetime of the module.
 */
static void
zswap_flush_pool(void)
{
	struct zswap_ent *e, *next;
	u_long i;

	mtx_lock(&zswap_mtx);
	for (i = 0; i < ZSWAP_HASH_SIZE; i++) {
		for (e = zswap_tab[i]; e != NULL; e = next) {
			next = e->ze_next;
			if (e->ze_spilled != 0) {
				zswap_slot_release_locked(e->ze_boff);
			} else {
				TAILQ_REMOVE(&zswap_lru, e, ze_lru);
				KASSERT(zswap_pool_bytes >=
				    e->ze_len + sizeof(*e),
				    ("zswap pool accounting"));
				zswap_pool_bytes -= e->ze_len + sizeof(*e);
			}
			/* The entry is unlinked now; a holder may still release it. */
			e->ze_spilled = 0;
			e->ze_len = 0;
			zswap_nent--;
			zswap_ent_release_locked(e);
		}
		zswap_tab[i] = NULL;
	}
	zswap_pool_bytes = 0;
	zswap_nent = 0;
	mtx_unlock(&zswap_mtx);
}

static int
zswap_d_open(struct disk *dp)
{
	int error;

	sx_slock(&zswap_cfg_lock);
	if (zswap_closing != 0 || zswap_bg_cp == NULL) {
		printf("zswap: no backing device configured, refusing open\n");
		error = ENXIO;
	} else {
		atomic_add_int(&zswap_refs, 1);
		error = 0;
	}
	sx_sunlock(&zswap_cfg_lock);
	return (error);
}

static int
zswap_d_close(struct disk *dp)
{

	/*
	 * The decrement and the pool flush must happen under the config lock:
	 * otherwise set_backing() (exclusive) can observe refs == 0 between
	 * the two, close the backing device and free the slot bitmap while
	 * zswap_flush_pool() is still walking entries and freeing slots
	 * against it (KASSERT panic / use-after-free).
	 */
	sx_slock(&zswap_cfg_lock);
	/* The value from fetchadd, not a re-read: two closers would race. */
	if (atomic_fetchadd_int(&zswap_refs, -1) == 1) {
		zswap_flush_pool();
		/* Nothing references the old slots any more. */
		atomic_store_rel_int(&zswap_dirty, 0);
	}
	sx_sunlock(&zswap_cfg_lock);
	return (0);
}

/* ------------------------------------------------------------------ */
/* sysctl                                                             */
/* ------------------------------------------------------------------ */

static void
zswap_pool_limits(void)
{

	zswap_pool_limit = (size_t)((uint64_t)physmem * PAGE_SIZE / 100 *
	    zswap_pool_percent);
	zswap_shr_trigger = zswap_pool_limit / 100 * zswap_shrinker_threshold;
	zswap_shr_target = zswap_pool_limit / 100 * zswap_shrinker_target;
}

static int
sysctl_zswap_level(SYSCTL_HANDLER_ARGS)
{
	int val, error;

	val = zswap_level;
	error = sysctl_handle_int(oidp, &val, 0, req);
	if (error != 0 || req->newptr == NULL)
		return (error);
	if (val < ZSWAP_LEVEL_MIN || val > ZSWAP_LEVEL_MAX)
		return (EINVAL);
	zswap_level = val;
	return (0);
}

static int
sysctl_zswap_pool_percent(SYSCTL_HANDLER_ARGS)
{
	int val, error;

	val = zswap_pool_percent;
	error = sysctl_handle_int(oidp, &val, 0, req);
	if (error != 0 || req->newptr == NULL)
		return (error);
	if (val < ZSWAP_POOL_PCT_MIN || val > ZSWAP_POOL_PCT_MAX)
		return (EINVAL);
	mtx_lock(&zswap_mtx);
	zswap_pool_percent = val;
	zswap_pool_limits();
	mtx_unlock(&zswap_mtx);
	return (0);
}

static int
sysctl_zswap_shr_pct(SYSCTL_HANDLER_ARGS)
{
	int val, error;
	int *pctl;

	pctl = (int *)arg1;
	val = *pctl;
	error = sysctl_handle_int(oidp, &val, 0, req);
	if (error != 0 || req->newptr == NULL)
		return (error);
	if (val < ZSWAP_SHR_PCT_MIN || val > ZSWAP_SHR_PCT_MAX)
		return (EINVAL);
	/* An inverted trigger/target pair would make the shrinker spin. */
	if (pctl == &zswap_shrinker_target && val >= zswap_shrinker_threshold)
		return (EINVAL);
	if (pctl == &zswap_shrinker_threshold && val <= zswap_shrinker_target)
		return (EINVAL);
	mtx_lock(&zswap_mtx);
	*pctl = val;
	zswap_pool_limits();
	mtx_unlock(&zswap_mtx);
	return (0);
}

static int
sysctl_zswap_threads(SYSCTL_HANDLER_ARGS)
{
	int val, error;

	val = zswap_nworkers;
	error = sysctl_handle_int(oidp, &val, 0, req);
	if (error != 0 || req->newptr == NULL)
		return (error);
	if (val < 1 || val > ZSWAP_THREADS_MAX)
		return (EINVAL);
	return (zswap_workers_resize(val));
}

static int
sysctl_zswap_backing(SYSCTL_HANDLER_ARGS)
{
	char buf[MAXPATHLEN];
	int error;

	strlcpy(buf, zswap_backing_path, sizeof(buf));
	error = sysctl_handle_string(oidp, buf, sizeof(buf), req);
	if (error != 0 || req->newptr == NULL)
		return (error);
	return (zswap_set_backing(buf));
}

/*
 * Write one page at offset 0, read it back and compare.  Returns the errno,
 * so "sysctl vfs.zswap.backing_selftest=1" doubles as a preflight check.
 */
static int
sysctl_zswap_selftest(SYSCTL_HANDLER_ARGS)
{
	char *wbuf, *rbuf;
	int val, error;

	val = 0;
	error = sysctl_handle_int(oidp, &val, 0, req);
	if (error != 0 || req->newptr == NULL)
		return (error);
	/*
	 * The selftest overwrites backing offset 0; while swap is active
	 * (or pages have been stored) that would corrupt the page at
	 * index 0.  The exclusive config lock also keeps d_open and
	 * set_backing out for the whole check-and-I/O sequence.
	 */
	sx_xlock(&zswap_cfg_lock);
	if (zswap_refs > 0 || atomic_load_acq_int(&zswap_dirty) != 0) {
		sx_xunlock(&zswap_cfg_lock);
		return (EBUSY);
	}
	if (zswap_bg_cp == NULL) {
		sx_xunlock(&zswap_cfg_lock);
		return (ENXIO);
	}

	wbuf = malloc(ZSWAP_BLOCK_SIZE, M_ZSWAP, M_WAITOK);
	rbuf = malloc(ZSWAP_BLOCK_SIZE, M_ZSWAP, M_WAITOK);
	memset(wbuf, 0xa5, ZSWAP_BLOCK_SIZE);
	memset(rbuf, 0, ZSWAP_BLOCK_SIZE);
	error = zswap_io_sync_locked(BIO_WRITE, wbuf, 0, ZSWAP_BLOCK_SIZE);
	if (error == 0)
		error = zswap_io_sync_locked(BIO_READ, rbuf, 0,
		    ZSWAP_BLOCK_SIZE);
	if (error == 0 && memcmp(wbuf, rbuf, ZSWAP_BLOCK_SIZE) != 0)
		error = EIO;
	printf("zswap: backing selftest -> %d\n", error);
	free(wbuf, M_ZSWAP);
	free(rbuf, M_ZSWAP);
	sx_xunlock(&zswap_cfg_lock);
	return (error);
}

static int
sysctl_zswap_ratio(SYSCTL_HANDLER_ARGS)
{
	uint64_t raw, compr;
	int val;

	raw = counter_u64_fetch(zswap_c_bytes_raw);
	compr = counter_u64_fetch(zswap_c_bytes_compr);
	if (compr == 0) {
		val = 0;
	} else {
		raw = raw * 100 / compr;
		val = (raw > (uint64_t)INT_MAX) ? INT_MAX : (int)raw;
	}
	return (sysctl_handle_int(oidp, &val, 0, req));
}

static int
sysctl_zswap_reset(SYSCTL_HANDLER_ARGS)
{
	int val, error;

	val = 0;
	error = sysctl_handle_int(oidp, &val, 0, req);
	if (error != 0 || req->newptr == NULL)
		return (error);

	counter_u64_zero(zswap_c_pages_out);
	counter_u64_zero(zswap_c_pages_in);
	counter_u64_zero(zswap_c_stored);
	counter_u64_zero(zswap_c_evicted);
	counter_u64_zero(zswap_c_compr_ok);
	counter_u64_zero(zswap_c_compr_fail);
	counter_u64_zero(zswap_c_not_worth);
	counter_u64_zero(zswap_c_passthru);
	counter_u64_zero(zswap_c_spill_write);
	counter_u64_zero(zswap_c_spill_read);
	counter_u64_zero(zswap_c_ram_read);
	counter_u64_zero(zswap_c_decompr_fail);
	counter_u64_zero(zswap_c_backing_err);
	counter_u64_zero(zswap_c_bytes_raw);
	counter_u64_zero(zswap_c_bytes_compr);
	counter_u64_zero(zswap_c_shr_evicted);
	counter_u64_zero(zswap_c_shr_err);
	counter_u64_zero(zswap_c_submit_fail);
	counter_u64_zero(zswap_c_keep_raw);
	counter_u64_zero(zswap_c_busy_fail);
	counter_u64_zero(zswap_c_backing_full);
	counter_u64_zero(zswap_c_missing);
	counter_u64_zero(zswap_c_slot_scans);
	counter_u64_zero(zswap_c_spill_bytes);
	counter_u64_zero(zswap_c_compress_waits);
	return (0);
}

SYSCTL_NODE(_vfs, OID_AUTO, zswap, CTLFLAG_RW, 0, "zswap compressed swap");
SYSCTL_NODE(_vfs_zswap, OID_AUTO, stats, CTLFLAG_RD, 0, "zswap statistics");

SYSCTL_INT(_vfs_zswap, OID_AUTO, enable, CTLFLAG_RWTUN, &zswap_enable, 0,
    "Compress pages (0 = pass through to backing device)");
SYSCTL_PROC(_vfs_zswap, OID_AUTO, compression_level,
    CTLTYPE_INT | CTLFLAG_RW | CTLFLAG_MPSAFE, NULL, 0,
    sysctl_zswap_level, "I", "zstd compression level (1-22)");
SYSCTL_PROC(_vfs_zswap, OID_AUTO, max_pool_percent,
    CTLTYPE_INT | CTLFLAG_RW | CTLFLAG_MPSAFE, NULL, 0,
    sysctl_zswap_pool_percent, "I",
    "Maximum percentage of physical memory used by the compressed pool");
SYSCTL_INT(_vfs_zswap, OID_AUTO, shrinker_enable, CTLFLAG_RWTUN,
    &zswap_shrinker_enable, 0, "Evict cold pages from the pool in background");
SYSCTL_PROC(_vfs_zswap, OID_AUTO, shrinker_threshold,
    CTLTYPE_INT | CTLFLAG_RW | CTLFLAG_MPSAFE, &zswap_shrinker_threshold, 0,
    sysctl_zswap_shr_pct, "I", "Pool usage percentage that wakes the shrinker");
SYSCTL_PROC(_vfs_zswap, OID_AUTO, shrinker_target,
    CTLTYPE_INT | CTLFLAG_RW | CTLFLAG_MPSAFE, &zswap_shrinker_target, 0,
    sysctl_zswap_shr_pct, "I", "Pool usage percentage to evict down to");
SYSCTL_PROC(_vfs_zswap, OID_AUTO, threads,
    CTLTYPE_INT | CTLFLAG_RW | CTLFLAG_MPSAFE, NULL, 0,
    sysctl_zswap_threads, "I",
    "Concurrent compression contexts (default: CPU count)");
SYSCTL_PROC(_vfs_zswap, OID_AUTO, backing,
    CTLTYPE_STRING | CTLFLAG_RW | CTLFLAG_MPSAFE, NULL, 0,
    sysctl_zswap_backing, "A", "Backing block device for spilled pages");
SYSCTL_PROC(_vfs_zswap, OID_AUTO, backing_selftest,
    CTLTYPE_INT | CTLFLAG_WR | CTLFLAG_MPSAFE, NULL, 0,
    sysctl_zswap_selftest, "I", "Write/read back one page at offset 0");
SYSCTL_U64(_vfs_zswap, OID_AUTO, mediasize, CTLFLAG_RD, &zswap_mediasize, 0,
    "Size of the zswap device in bytes");

SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, pages_out, CTLFLAG_RD,
    &zswap_c_pages_out, "Pages written by the swap pager");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, pages_in, CTLFLAG_RD,
    &zswap_c_pages_in, "Pages read by the swap pager");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, stored, CTLFLAG_RD,
    &zswap_c_stored, "Pages stored compressed in RAM");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, evicted, CTLFLAG_RD,
    &zswap_c_evicted, "Compressed pages dropped");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, compr_ok, CTLFLAG_RD,
    &zswap_c_compr_ok, "Successful compression attempts");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, compr_fail, CTLFLAG_RD,
    &zswap_c_compr_fail, "Failed compression attempts");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, not_worth, CTLFLAG_RD,
    &zswap_c_not_worth, "Pages that did not shrink or did not fit the pool");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, passthru, CTLFLAG_RD,
    &zswap_c_passthru, "Pages written while compression was disabled");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, spill_write, CTLFLAG_RD,
    &zswap_c_spill_write, "Pages written to the backing device");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, spill_read, CTLFLAG_RD,
    &zswap_c_spill_read, "Pages read from the backing device");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, ram_read, CTLFLAG_RD,
    &zswap_c_ram_read, "Pages served from the compressed pool");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, decompr_fail, CTLFLAG_RD,
    &zswap_c_decompr_fail, "Failed decompressions");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, backing_err, CTLFLAG_RD,
    &zswap_c_backing_err, "Backing device I/O errors");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, bytes_raw, CTLFLAG_RD,
    &zswap_c_bytes_raw, "Bytes presented to the compressor");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, bytes_compr, CTLFLAG_RD,
    &zswap_c_bytes_compr, "Bytes produced by the compressor");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, shrinker_evicted, CTLFLAG_RD,
    &zswap_c_shr_evicted, "Pages evicted by the LRU shrinker");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, shrinker_err, CTLFLAG_RD,
    &zswap_c_shr_err, "Shrinker writeback failures");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, submit_fail, CTLFLAG_RD,
    &zswap_c_submit_fail,
    "Backing submissions that failed (no bio or backing reconfiguring)");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, keep_raw, CTLFLAG_RD,
    &zswap_c_keep_raw, "Pages kept resident to avoid writeback races");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, backing_full, CTLFLAG_RD,
    &zswap_c_backing_full, "Pages that found no free backing slot");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, spill_bytes, CTLFLAG_RD,
    &zswap_c_spill_bytes, "Payload bytes written to / read from the backing device");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, slot_scans, CTLFLAG_RD,
    &zswap_c_slot_scans, "Level-2 bitmap words walked per allocation");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, missing, CTLFLAG_RD,
    &zswap_c_missing, "Reads for a page with no entry");
SYSCTL_U64(_vfs_zswap_stats, OID_AUTO, backing_slots, CTLFLAG_RD,
    &zswap_nslots, 0, "Backing slots in total");
SYSCTL_U64(_vfs_zswap_stats, OID_AUTO, backing_free, CTLFLAG_RD,
    &zswap_nfree, 0, "Backing slots still free");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, busy_fail, CTLFLAG_RD,
    &zswap_c_busy_fail,
    "Writes failed because a writeback was in flight (pager retries)");
SYSCTL_COUNTER_U64(_vfs_zswap_stats, OID_AUTO, compress_waits, CTLFLAG_RD,
    &zswap_c_compress_waits,
    "Acquires that had to wait for a free compression context");
SYSCTL_PROC(_vfs_zswap_stats, OID_AUTO, compr_ratio_x100,
    CTLTYPE_INT | CTLFLAG_RD | CTLFLAG_MPSAFE, NULL, 0,
    sysctl_zswap_ratio, "I", "Compression ratio multiplied by 100");
SYSCTL_PROC(_vfs_zswap_stats, OID_AUTO, reset,
    CTLTYPE_INT | CTLFLAG_WR | CTLFLAG_MPSAFE, NULL, 0,
    sysctl_zswap_reset, "I", "Write anything to reset all counters");
SYSCTL_ULONG(_vfs_zswap_stats, OID_AUTO, entries, CTLFLAG_RD, &zswap_nent, 0,
    "Compressed pages currently in the pool");
SYSCTL_ULONG(_vfs_zswap_stats, OID_AUTO, pool_bytes, CTLFLAG_RD,
    &zswap_pool_bytes, 0, "Bytes used by the compressed pool");
SYSCTL_ULONG(_vfs_zswap_stats, OID_AUTO, pool_limit, CTLFLAG_RD,
    &zswap_pool_limit, 0, "Pool size limit in bytes");
SYSCTL_ULONG(_vfs_zswap_stats, OID_AUTO, shrinker_trigger_bytes, CTLFLAG_RD,
    &zswap_shr_trigger, 0, "Pool bytes that wake the shrinker");
SYSCTL_ULONG(_vfs_zswap_stats, OID_AUTO, shrinker_target_bytes, CTLFLAG_RD,
    &zswap_shr_target, 0, "Pool bytes the shrinker evicts down to");

/* ------------------------------------------------------------------ */
/* module                                                             */
/* ------------------------------------------------------------------ */

static void
zswap_counters_alloc(void)
{

	zswap_c_pages_out = counter_u64_alloc(M_WAITOK);
	zswap_c_pages_in = counter_u64_alloc(M_WAITOK);
	zswap_c_stored = counter_u64_alloc(M_WAITOK);
	zswap_c_evicted = counter_u64_alloc(M_WAITOK);
	zswap_c_compr_ok = counter_u64_alloc(M_WAITOK);
	zswap_c_compr_fail = counter_u64_alloc(M_WAITOK);
	zswap_c_not_worth = counter_u64_alloc(M_WAITOK);
	zswap_c_passthru = counter_u64_alloc(M_WAITOK);
	zswap_c_spill_write = counter_u64_alloc(M_WAITOK);
	zswap_c_spill_read = counter_u64_alloc(M_WAITOK);
	zswap_c_ram_read = counter_u64_alloc(M_WAITOK);
	zswap_c_decompr_fail = counter_u64_alloc(M_WAITOK);
	zswap_c_backing_err = counter_u64_alloc(M_WAITOK);
	zswap_c_bytes_raw = counter_u64_alloc(M_WAITOK);
	zswap_c_bytes_compr = counter_u64_alloc(M_WAITOK);
	zswap_c_shr_evicted = counter_u64_alloc(M_WAITOK);
	zswap_c_shr_err = counter_u64_alloc(M_WAITOK);
	zswap_c_submit_fail = counter_u64_alloc(M_WAITOK);
	zswap_c_keep_raw = counter_u64_alloc(M_WAITOK);
	zswap_c_busy_fail = counter_u64_alloc(M_WAITOK);
	zswap_c_backing_full = counter_u64_alloc(M_WAITOK);
	zswap_c_missing = counter_u64_alloc(M_WAITOK);
	zswap_c_slot_scans = counter_u64_alloc(M_WAITOK);
	zswap_c_spill_bytes = counter_u64_alloc(M_WAITOK);
	zswap_c_compress_waits = counter_u64_alloc(M_WAITOK);
}

static void
zswap_counters_free(void)
{

	counter_u64_free(zswap_c_pages_out);
	counter_u64_free(zswap_c_pages_in);
	counter_u64_free(zswap_c_stored);
	counter_u64_free(zswap_c_evicted);
	counter_u64_free(zswap_c_compr_ok);
	counter_u64_free(zswap_c_compr_fail);
	counter_u64_free(zswap_c_not_worth);
	counter_u64_free(zswap_c_passthru);
	counter_u64_free(zswap_c_spill_write);
	counter_u64_free(zswap_c_spill_read);
	counter_u64_free(zswap_c_ram_read);
	counter_u64_free(zswap_c_decompr_fail);
	counter_u64_free(zswap_c_backing_err);
	counter_u64_free(zswap_c_bytes_raw);
	counter_u64_free(zswap_c_bytes_compr);
	counter_u64_free(zswap_c_shr_evicted);
	counter_u64_free(zswap_c_shr_err);
	counter_u64_free(zswap_c_submit_fail);
	counter_u64_free(zswap_c_keep_raw);
	counter_u64_free(zswap_c_busy_fail);
	counter_u64_free(zswap_c_backing_full);
	counter_u64_free(zswap_c_missing);
	counter_u64_free(zswap_c_slot_scans);
	counter_u64_free(zswap_c_spill_bytes);
	counter_u64_free(zswap_c_compress_waits);
}

static int
zswap_init(void)
{
	char path[MAXPATHLEN];
	u_long mediasize_mb;
	int error, nthreads, i;

	/*
	 * 64-bit arithmetic on purpose: on a 32-bit kernel physmem * PAGE_SIZE
	 * wraps for >= 4 GB of RAM, and 16UL << 30 is undefined.
	 */
	mediasize_mb = (u_long)(MIN((uint64_t)physmem * PAGE_SIZE / 2,
	    ZSWAP_MEDIASIZE_MAX) >> 20);
	if (mediasize_mb < (u_long)(ZSWAP_MEDIASIZE_MIN >> 20))
		mediasize_mb = (u_long)(ZSWAP_MEDIASIZE_MIN >> 20);

	TUNABLE_INT_FETCH("zswap.enable", &zswap_enable);
	TUNABLE_INT_FETCH("zswap.compression_level", &zswap_level);
	TUNABLE_INT_FETCH("zswap.max_pool_percent", &zswap_pool_percent);
	TUNABLE_INT_FETCH("zswap.shrinker_enable", &zswap_shrinker_enable);
	TUNABLE_INT_FETCH("zswap.shrinker_threshold", &zswap_shrinker_threshold);
	TUNABLE_INT_FETCH("zswap.shrinker_target", &zswap_shrinker_target);
	TUNABLE_ULONG_FETCH("zswap.mediasize_mb", &mediasize_mb);
	path[0] = '\0';
	TUNABLE_STR_FETCH("zswap.backing", path, sizeof(path));

	if (zswap_level < ZSWAP_LEVEL_MIN || zswap_level > ZSWAP_LEVEL_MAX)
		zswap_level = 3;
	if (zswap_pool_percent < ZSWAP_POOL_PCT_MIN ||
	    zswap_pool_percent > ZSWAP_POOL_PCT_MAX)
		zswap_pool_percent = 20;
	if (zswap_shrinker_threshold < ZSWAP_SHR_PCT_MIN ||
	    zswap_shrinker_threshold > ZSWAP_SHR_PCT_MAX)
		zswap_shrinker_threshold = 80;
	if (zswap_shrinker_target < ZSWAP_SHR_PCT_MIN ||
	    zswap_shrinker_target > ZSWAP_SHR_PCT_MAX)
		zswap_shrinker_target = 60;
	if (zswap_shrinker_target >= zswap_shrinker_threshold)
		zswap_shrinker_threshold =
		    MIN(zswap_shrinker_target + 10, ZSWAP_SHR_PCT_MAX);

	zswap_mediasize = (off_t)mediasize_mb << 20;

	mtx_init(&zswap_mtx, "zswap", NULL, MTX_DEF);
	mtx_init(&zswap_rd_mtx, "zswaprd", NULL, MTX_DEF);
	for (i = 0; i < ZSWAP_NGUARD; i++)
		LIST_INIT(&zswap_guards[i]);
	/* Initialized first: the MOD_LOAD failure path tears them down. */
	sx_init(&zswap_wlock, "zswapw");
	sx_init(&zswap_cfg_lock, "zswapcfg");
	zswap_closing = 0;
	zswap_pool_limits();
	zswap_tab = malloc(ZSWAP_HASH_SIZE * sizeof(*zswap_tab), M_ZSWAP,
	    M_WAITOK | M_ZERO);
	zswap_counters_alloc();
	zswap_rdctx = zswap_zstd_create();
	if (zswap_rdctx == NULL) {
		printf("zswap: cannot allocate zstd contexts\n");
		return (ENOMEM);
	}
	nthreads = mp_ncpus;
	TUNABLE_INT_FETCH("zswap.threads", &nthreads);
	if (nthreads < 1)
		nthreads = 1;
	if (nthreads > ZSWAP_THREADS_MAX)
		nthreads = ZSWAP_THREADS_MAX;
	if (zswap_workers_resize(nthreads) != 0) {
		printf("zswap: cannot allocate %d compression contexts\n",
		    nthreads);
		return (ENOMEM);
	}

	/*
	 * Decompression of spilled pages runs here, not in d_strategy and not
	 * in the bio callback: both may be in a critical section, and zstd
	 * allocates its workspace.  A fast queue is required because it is
	 * enqueued from a bio completion.
	 */
	zswap_tq = taskqueue_create_fast("zswapdec", M_WAITOK,
	    taskqueue_thread_enqueue, &zswap_tq);
	if (zswap_tq == NULL) {
		printf("zswap: cannot create decompression taskqueue\n");
		return (ENOMEM);
	}
	/*
	 * Two threads: one may be parked in zswap_readwait_fn() waiting for
	 * a write to land, while the other still drains decompression tasks.
	 */
	taskqueue_start_threads(&zswap_tq, 2, PI_DISK, "zswapdec");

	if (path[0] != '\0' && zswap_set_backing(path) != 0)
		printf("zswap: cannot use backing device %s\n", path);

	zswap_shr_buf = malloc(ZSWAP_BLOCK_SIZE, M_ZSWAP, M_WAITOK);
	sema_init(&zswap_shr_wake, 0, "zswapwake");
	sema_init(&zswap_shr_done, 0, "zswapdone");
	zswap_semas_inited = 1;
	zswap_shr_running = 1;
	error = kproc_create(zswap_shrinker, NULL, &zswap_shr_proc, 0, 0,
	    "zswap_shrinker");
	if (error != 0) {
		printf("zswap: cannot start shrinker thread (%d)\n", error);
		zswap_shr_running = 0;
		free(zswap_shr_buf, M_ZSWAP);
		zswap_shr_buf = NULL;
	}

	zswap_dp = disk_alloc();
	zswap_dp->d_name = "zswap";
	zswap_dp->d_unit = 0;
	zswap_dp->d_open = zswap_d_open;
	zswap_dp->d_close = zswap_d_close;
	zswap_dp->d_strategy = zswap_strategy;
	zswap_dp->d_mediasize = zswap_mediasize;
	zswap_dp->d_sectorsize = ZSWAP_SECTORSIZE;
	zswap_dp->d_maxsize = 128 * 1024;
	/*
	 * Without these flags g_disk_start rejects BIO_FLUSH and BIO_DELETE
	 * with EOPNOTSUPP before they ever reach zswap_strategy().
	 */
	zswap_dp->d_flags = DISKFLAG_CANDELETE | DISKFLAG_CANFLUSHCACHE;
	disk_create(zswap_dp, DISK_VERSION);

	printf("zswap: %jdMB device, level %d, pool %zuMB "
	    "(shrinker %d%%->%d%%), backing %s\n",
	    (intmax_t)(zswap_mediasize >> 20), zswap_level,
	    zswap_pool_limit >> 20, zswap_shrinker_threshold,
	    zswap_shrinker_target,
	    zswap_backing_path[0] != '\0' ? zswap_backing_path : "(none)");
	return (0);
}

static int
zswap_fini(void)
{
	struct zswap_ent *e, *next;
	u_long i;

	/*
	 * Block new opens and backing reconfiguration, then check the
	 * reference count: while the config lock is held exclusively the
	 * count is stable, since d_open needs it shared.
	 */
	sx_xlock(&zswap_cfg_lock);
	if (zswap_refs > 0) {
		sx_xunlock(&zswap_cfg_lock);
		return (EBUSY);
	}
	zswap_closing = 1;
	sx_xunlock(&zswap_cfg_lock);

	/*
	 * Stop the shrinker before touching the config lock again: the
	 * shrinker sleeps on backing I/O while holding it shared.  The
	 * drain loop checks zswap_shutdown under the pool mutex, so a
	 * long drain is cut short here.
	 */
	if (zswap_shr_running != 0) {
		mtx_lock(&zswap_mtx);
		zswap_shutdown = 1;
		mtx_unlock(&zswap_mtx);
		sema_post(&zswap_shr_wake);
		sema_wait(&zswap_shr_done);
		zswap_shr_running = 0;
	}
	if (zswap_shr_buf != NULL) {
		free(zswap_shr_buf, M_ZSWAP);
		zswap_shr_buf = NULL;
	}

	if (zswap_dp != NULL) {
		disk_destroy(zswap_dp);
		zswap_dp = NULL;
	}

	mtx_lock(&zswap_mtx);
	for (i = 0; i < ZSWAP_HASH_SIZE; i++) {
		for (e = zswap_tab[i]; e != NULL; e = next) {
			next = e->ze_next;
			free(e->ze_data, M_ZSWAP);
			free(e, M_ZSWAP);
		}
		zswap_tab[i] = NULL;
	}
	zswap_nent = 0;
	zswap_pool_bytes = 0;
	mtx_unlock(&zswap_mtx);

	/*
	 * refs == 0 proves no new bios arrive, but children of earlier
	 * bios can still be in flight; wait them out before destroying
	 * the GEOM consumer they complete against.
	 */
	sx_xlock(&zswap_cfg_lock);
	while (atomic_load_acq_int(&zswap_children) != 0)
		pause("zswapdrain", hz / 10);
	zswap_backing_close_locked();
	sx_xunlock(&zswap_cfg_lock);

	/*
	 * No child bio is in flight, so no new decompression task can be
	 * queued; wait for the ones already queued, then tear the queue down
	 * before the context they use goes away.
	 */
	while (atomic_load_acq_int(&zswap_tasks) != 0)
		pause("zswapdtask", hz / 10);
	if (zswap_tq != NULL) {
		taskqueue_free(zswap_tq);
		zswap_tq = NULL;
	}

	zswap_workers_free();
	zswap_zstd_destroy(zswap_rdctx);
	zswap_rdctx = NULL;
	free(zswap_tab, M_ZSWAP);
	if (zswap_semas_inited != 0) {
		sema_destroy(&zswap_shr_wake);
		sema_destroy(&zswap_shr_done);
		zswap_semas_inited = 0;
	}
	zswap_counters_free();
	sx_destroy(&zswap_wlock);
	sx_destroy(&zswap_cfg_lock);
	mtx_destroy(&zswap_rd_mtx);
	mtx_destroy(&zswap_mtx);
	return (0);
}

static int
zswap_modevent(module_t mod, int type, void *arg)
{
	int error;

	switch (type) {
	case MOD_LOAD:
		error = zswap_init();
		if (error != 0) {
			zswap_fini();
			return (error);
		}
		return (0);
	case MOD_UNLOAD:
		return (zswap_fini());
	default:
		return (EOPNOTSUPP);
	}
}

static moduledata_t zswap_mod = {
	"zswap",
	zswap_modevent,
	NULL
};

DECLARE_MODULE(zswap, zswap_mod, SI_SUB_DRIVERS, SI_ORDER_MIDDLE);
MODULE_VERSION(zswap, 2);
