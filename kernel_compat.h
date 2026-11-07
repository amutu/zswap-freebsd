/*-
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Force-included into every translation unit of this module.
 *
 * The kernel build is freestanding and the zstd sources expect a couple of
 * names that only exist in hosted C.  NULL comes for free in userland via
 * <stddef.h>; the FreeBSD kernel headers define __ptrdiff_t but not the
 * unadorned name.
 */

#ifndef _ZSWAP_KERNEL_COMPAT_H_
#define	_ZSWAP_KERNEL_COMPAT_H_

#include <sys/types.h>

#ifndef NULL
#define	NULL	((void *)0)
#endif

typedef __ptrdiff_t ptrdiff_t;

#endif /* !_ZSWAP_KERNEL_COMPAT_H_ */
