/*-
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * zswap - zstd compression back end.
 *
 * The zstd sources are the ones shipped with the FreeBSD base system for
 * OpenZFS (sys/contrib/openzfs/module/zstd).  They are already built for a
 * freestanding kernel environment: allocation goes through ZSTD_customMem,
 * so no libc dependency is introduced.
 *
 * Contexts are created per worker so that compression can run in parallel;
 * liveness is the caller's problem (see zswap.c).
 */

#include <sys/param.h>
#include <sys/systm.h>
#include <sys/malloc.h>
#include <sys/errno.h>

#ifndef ZSTD_STATIC_LINKING_ONLY
#define	ZSTD_STATIC_LINKING_ONLY
#endif
#include <zstd.h>

#include "zswap.h"

struct zswap_zctx {
	ZSTD_CCtx	*zc_cctx;
	ZSTD_DCtx	*zc_dctx;
};

/*
 * Kernel allocations in the page-out path must not sleep (M_WAITOK could
 * deadlock against the VM that is waiting for this very page-out), so all
 * zstd workspace allocations use M_NOWAIT.  zswap_zstd_create() pre-warms
 * the workspace at the maximum level so that the allocations actually
 * happen in process context (init / sysctl), where a failure is harmless
 * and simply retried on the next compression.
 */
static void *
zswap_zalloc(void *opaque, size_t size)
{

	return (malloc(size, M_ZSWAP, M_NOWAIT | M_ZERO));
}

static void
zswap_zfree(void *opaque, void *ptr)
{

	free(ptr, M_ZSWAP);
}

zswap_zctx *
zswap_zstd_create(void)
{
	static const ZSTD_customMem mem = {
		.customAlloc = zswap_zalloc,
		.customFree = zswap_zfree,
		.opaque = NULL,
	};
	zswap_zctx *zc;
	char src[64], dst[256];

	zc = malloc(sizeof(*zc), M_ZSWAP, M_WAITOK | M_ZERO);
	zc->zc_cctx = ZSTD_createCCtx_advanced(mem);
	zc->zc_dctx = ZSTD_createDCtx_advanced(mem);
	if (zc->zc_cctx == NULL || zc->zc_dctx == NULL) {
		zswap_zstd_destroy(zc);
		return (NULL);
	}
	/*
	 * Pre-allocate the compression workspace now, in sleepable context.
	 * zstd sizes the workspace from the level and the pledged source
	 * size and only ever grows it, so warming up at the maximum level
	 * with a full-page pledged size covers every later compress call
	 * (same page size, same or lower level) without allocating in the
	 * page-out path.  A failure is ignored: the first real compression
	 * simply retries the allocation and spills the page on failure.
	 */
	memset(src, 0, sizeof(src));
	(void)ZSTD_CCtx_setParameter(zc->zc_cctx, ZSTD_c_compressionLevel,
	    ZSTD_maxCLevel());
	(void)ZSTD_CCtx_setPledgedSrcSize(zc->zc_cctx, ZSWAP_BLOCK_SIZE);
	(void)ZSTD_compress2(zc->zc_cctx, dst, sizeof(dst), src, sizeof(src));
	return (zc);
}

void
zswap_zstd_destroy(zswap_zctx *zc)
{

	if (zc == NULL)
		return;
	if (zc->zc_cctx != NULL)
		ZSTD_freeCCtx(zc->zc_cctx);
	if (zc->zc_dctx != NULL)
		ZSTD_freeDCtx(zc->zc_dctx);
	free(zc, M_ZSWAP);
}

int
zswap_zstd_compress(zswap_zctx *zc, void *dst, size_t dstcap, const void *src,
    size_t srclen, int level, size_t *outlen)
{
	size_t r;

	if (zc == NULL || zc->zc_cctx == NULL)
		return (ENXIO);
	r = ZSTD_compressCCtx(zc->zc_cctx, dst, dstcap, src, srclen, level);
	if (ZSTD_isError(r))
		return (EIO);
	*outlen = r;
	return (0);
}

int
zswap_zstd_decompress(zswap_zctx *zc, void *dst, size_t dstcap, const void *src,
    size_t srclen, size_t *outlen)
{
	size_t r;

	if (zc == NULL || zc->zc_dctx == NULL)
		return (ENXIO);
	r = ZSTD_decompressDCtx(zc->zc_dctx, dst, dstcap, src, srclen);
	if (ZSTD_isError(r))
		return (EIO);
	*outlen = r;
	return (0);
}
