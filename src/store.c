/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * NOTE that all functions should be thread-safe.
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <arpa/inet.h>
#include <endian.h>
#include <stdio.h>
#include <pthread.h>

#include "anfs.h"

/**
 * copier implementation.
 * TODO: re-implement this using aio- library.
 */

static const unsigned idle_sleep = 300;
static char path_src[PATH_MAX];
static char path_dest[PATH_MAX];
static char cpbuf[128*(1<<20)];

static inline void rq_lock(struct copy_queue *rq)
{
	pthread_mutex_lock(&rq->lock);
}

static inline void rq_unlock(struct copy_queue *rq)
{
	pthread_mutex_unlock(&rq->lock);
}

static inline
struct anfs_copy_request *fetch_request(struct copy_queue *rq)
{
	struct anfs_copy_request *req = NULL;

	rq_lock(rq);
	if (list_empty(&rq->list))
		goto out;

	req = list_first_entry(&rq->list, struct anfs_copy_request, list);
	list_del(&req->list);

out:
	rq_unlock(rq);
	return req;
}

static void copy_file(struct anfs_copy_request *req, char *dest, char *src)
{
	int ret = 0;
	uint64_t oid;
	size_t n;
	FILE *fpin, *fpout;
	struct anfs_data_file *file = req->file;

	if (NULL == (fpin = fopen(src, "r"))) {
		ret = -errno;
		goto out_err;
	}
	if (NULL == (fpout = fopen(dest, "w"))) {
		ret = -errno;
		goto out_err;
	}

	while ((n = fread(cpbuf, sizeof(char), sizeof(cpbuf), fpin)) > 0) {
		if (fwrite(cpbuf, sizeof(char), n, fpout) != n)
			goto out_fpout;
	}

	oid = get_file_ino(dest) + ANFS_OBJECT_OFFSET;

	if (file) {
		file->osd = req->dest;
		file->oid = oid;
	}

	req->oid = oid;

out_fpout:
	fclose(fpout);
out_fpin:
	fclose(fpin);
out_err:
	(*req->callback) (ret, req);
}

static void *copier(void *arg)
{
	uint64_t ino;
	int src, dest;
	struct anfs_copy_request *req;
	struct copy_queue *rq = (struct copy_queue *) arg;
	struct anfs_ctx *ctx = rq->ctx;

	while (1) {
		if (NULL == (req = fetch_request(rq))) {
			usleep(idle_sleep);
			continue;
		}

		ino = req->ino;
		src = req->src;
		dest = req->dest;
		anfs_store_get_path(anfs_store(ctx), ino, &src, path_src);
		anfs_store_get_path(anfs_store(ctx), ino, &dest, path_dest);

		copy_file(req, path_dest, path_src);
	}

out:
	return (void *) 0;
}

int anfs_store_init(struct anfs_store *self, const int ndev, char **backends)
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

	/** initialize the copier thread. */
	self->rq.ctx = anfs_ctx(self, store);
	pthread_mutex_init(&self->rq.lock, NULL);
	INIT_LIST_HEAD(&self->rq.list);

	ret = pthread_create(&self->copier, NULL, &copier, &self->rq);
	if (ret)
		return -errno;

	return 0;
}

int anfs_store_exit(struct anfs_store *self)
{
	int ret;
	void *res;

	pthread_cancel(self->copier);
	pthread_join(self->copier, &res);
	pthread_mutex_destroy(&self->rq.lock);

	return 0;
}

int anfs_store_create(struct anfs_store *self, uint64_t ino, int *index)
{
	int ret = 0;
	FILE *fp;
	char pathbuf[PATH_MAX];
	struct anfs_ctx *ctx = anfs_ctx(self, store);

	anfs_store_get_path(self, ino, index, pathbuf);
	fp = fopen(pathbuf, "w");
	if (!fp)
		ret = -errno;
	else
		fclose(fp);

	return ret;
}

