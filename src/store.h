/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * 
 */
#ifndef	__ANFS_STORE_H__
#define	__ANFS_STORE_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>

struct anfs_store {
	int n_backends;
	const char *backends[ANFS_MAX_DEV];
};

static inline int anfs_store_init(struct anfs_store *self, const int ndev,
				char **backends)
{
	int i, ret = 0;
	uint8_t d;
	char pathbuf[PATH_MAX];

	if (!self)
		return -EINVAL;

	self->n_backends = ndev;

	for (i = 0; i < ndev; i++) {
		self->backends[i] = backends[i];

		for (d = 0; d < 0xff; d++) {
			sprintf(pathbuf, "%s/%02x", self->backends[i], d);
			ret = mkdir(pathbuf, 0755);
			if (ret && errno != EEXIST)
				return -errno;
		}
	}

	return 0;
}

/** @index: pass -1 to use the default location (caculated by modular to ino) */
static inline
void anfs_store_get_path(struct anfs_store *self, uint64_t ino, int index,
			char *buf)
{
	int loc = index < 0 ? ino % self->n_backends : index;

	sprintf(buf, "%s/%02x/%016llx.anfs", self->backends[loc],
			(uint8_t) (ino & 0xffUL), anfs_llu(ino));
}

static inline int anfs_store_exit(struct anfs_store *self)
{
	return 0;
}

static inline
int anfs_store_open(struct anfs_store *self, uint64_t ino, int index,
			int flags)
{
	char pathbuf[PATH_MAX];
	anfs_store_get_path(self, ino, index, pathbuf);
	return open(pathbuf, flags);
}

/** pass -1 as index for default file location */
static inline
int anfs_store_create(struct anfs_store *self, uint64_t ino, int index)
{
	FILE *fp;
	char pathbuf[PATH_MAX];

	anfs_store_get_path(self, ino, index, pathbuf);
	fp = fopen(pathbuf, "w");
	if (!fp)
		return -errno;
	fclose(fp);
	return 0;
}

static inline
int anfs_store_truncate(struct anfs_store *self, uint64_t ino, int index,
			uint64_t newsize)
{
	char pathbuf[PATH_MAX];
	anfs_store_get_path(self, ino, index, pathbuf);
	return truncate(pathbuf, (off_t) newsize);
}

#endif

