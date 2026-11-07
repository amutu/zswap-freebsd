#!/bin/sh

kenv zswap.mediasize_mb=3072          # physmem/2=1G 不够，手动给到 1.25×RAM
kldload zswap
geli onetime -d /dev/da0p2
sysctl vfs.zswap.backing=da0p2.eli     # 或 vnode md
sysctl vfs.zswap.compression_level=3
sysctl vfs.zswap.max_pool_percent=40
sysctl vfs.zswap.shrinker_threshold=70
sysctl vfs.zswap.shrinker_target=50
sysctl vfs.zswap.threads=2       # vCPU 数
swapon /dev/zswap0
