#
# zswap.ko - compressed swap cache for FreeBSD
#
# Builds against the running kernel's source tree (/usr/src).
#   make            -> zswap.ko
#   make install    -> /boot/modules/zswap.ko
#
KMOD=	zswap
SRCS=	zswap.c zswap_zstd.c vnode_if.h

# Kernel-safe zstd shipped with the base system for OpenZFS.
ZSTD_LIB?=	${SYSDIR}/contrib/openzfs/module/zstd/lib
ZSTD_INC?=	${SYSDIR}/contrib/openzfs/module/zstd/include

# lib/common
SRCS+=	entropy_common.c error_private.c fse_decompress.c pool.c \
	xxhash.c zstd_common.c
# lib/compress
SRCS+=	fse_compress.c hist.c huf_compress.c zstd_compress.c \
	zstd_compress_literals.c zstd_compress_sequences.c \
	zstd_compress_superblock.c zstd_double_fast.c zstd_fast.c \
	zstd_lazy.c zstd_ldm.c zstd_opt.c
# lib/decompress
SRCS+=	huf_decompress.c zstd_ddict.c zstd_decompress.c \
	zstd_decompress_block.c

CFLAGS+=	-I${ZSTD_LIB} -I${ZSTD_LIB}/common \
		-I${ZSTD_LIB}/compress -I${ZSTD_LIB}/decompress -I${ZSTD_INC}
CFLAGS+=	-include ${.CURDIR}/kernel_compat.h
CFLAGS+=	-DZSTD_DISABLE_ASM -DNDEBUG \
		-DDEBUGLEVEL=0
#CFLAGS+=	-g
CFLAGS+=	-Wno-unused-function -Wno-unused-variable -Wno-sign-compare \
		-Wno-missing-field-initializers

.include <bsd.kmod.mk>

# SYSDIR is only known after bsd.kmod.mk has been read.
.PATH: ${ZSTD_LIB}/common ${ZSTD_LIB}/compress ${ZSTD_LIB}/decompress
