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
#include <pthread.h>
#include <errno.h>

struct copy_queue {
	struct anfs_ctx *ctx;
	pthread_mutex_t lock;	/* copier request queue lock */
	struct list_head list;	/* copier request queue */
};

struct anfs_store {
	struct copy_queue rq;
	pthread_t copier;
	int n_backends;
	const char *backends[ANFS_MAX_DEV];
};

int anfs_store_init(struct anfs_store *self, const int ndev, char **backends);

int anfs_store_exit(struct anfs_store *self);

/** @index: pass -1 to use the default location (caculated by modular to ino)
 */
static inline
void anfs_store_get_path(struct anfs_store *self, uint64_t ino, int *index,
			char *buf)
{
	int loc = *index < 0 ? ino % self->n_backends : *index;

	if (ino < ANFS_INO_NORMAL) {	/* don't for virtual entries */
		*buf = 0;
		return;
	}

	*index = loc;
	sprintf(buf, "%s/%02x/%016llx.anfs", self->backends[loc],
			(uint8_t) (ino & 0xffUL), anfs_llu(ino));
}

static inline
int anfs_store_open(struct anfs_store *self, uint64_t ino, int index,
			int flags)
{
	int pindex = index;
	char pathbuf[PATH_MAX];
	anfs_store_get_path(self, ino, &pindex, pathbuf);
	return open(pathbuf, flags);
}

/** pass -1 as *index for default file location. the index will be set by
 * this function */
int anfs_store_create(struct anfs_store *self, uint64_t ino, int *index);

static inline
int anfs_store_truncate(struct anfs_store *self, uint64_t ino, int index,
			uint64_t newsize)
{
	int pindex = index;
	char pathbuf[PATH_MAX];
	anfs_store_get_path(self, ino, &pindex, pathbuf);
	return truncate(pathbuf, (off_t) newsize);
}

struct anfs_copy_request;
typedef void (*anfs_copy_callback_t) (int status,
				struct anfs_copy_request *req);

struct anfs_copy_request {
	uint64_t ino;
	int src;
	int dest;
	anfs_copy_callback_t callback;
	struct anfs_task *task;
	struct anfs_data_file *file;
	void *priv;
	uint64_t oid;		/* object id of newly replicated one */
	uint64_t t_submit;
	uint64_t t_complete;

	struct list_head list;	/* private: internal use only */
};

static inline void anfs_store_request_copy(struct anfs_store *self,
						struct anfs_copy_request *req)
{
	struct copy_queue *rq = &self->rq;

	pthread_mutex_lock(&rq->lock);
	list_add_tail(&req->list, &rq->list);
	pthread_mutex_unlock(&rq->lock);
}

#endif

