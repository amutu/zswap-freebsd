/*-
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * zswap - compressed swap cache for FreeBSD (loadable module)
 *
 * Internal interface between the swap device front end (zswap.c) and the
 * zstd compression back end (zswap_zstd.c).
 */

#ifndef _ZSWAP_H_
#define	_ZSWAP_H_

#include <sys/param.h>
#include <sys/types.h>
#include <sys/malloc.h>

MALLOC_DECLARE(M_ZSWAP);

/*
 * The device is exposed with a 512 byte sector size, but all real I/O is
 * page sized: the compressed pool is indexed by page.
 */
#define	ZSWAP_SECTORSIZE	512
#define	ZSWAP_BLOCK_SIZE	PAGE_SIZE

/*
 * A compression context pair.  Contexts are not internally locked: the
 * caller guarantees exclusive use (a worker mutex, or the pool mutex for
 * the read side).
 */
typedef struct zswap_zctx zswap_zctx;

zswap_zctx	*zswap_zstd_create(void);
void		 zswap_zstd_destroy(zswap_zctx *);
int		 zswap_zstd_compress(zswap_zctx *, void *dst, size_t dstcap,
		    const void *src, size_t srclen, int level, size_t *outlen);
int		 zswap_zstd_decompress(zswap_zctx *, void *dst, size_t dstcap,
		    const void *src, size_t srclen, size_t *outlen);

#endif /* !_ZSWAP_H_ */
