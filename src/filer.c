/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * filer implementation.
 */
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include "activefs.h"

typedef int (*afs_filer_handler_t) (struct afs_ctx *ctx,
					struct afs_filer_request *req);

struct afs_filer {
	const char *name;

	afs_filer_handler_t create;
	afs_filer_handler_t read;
	afs_filer_handler_t write;
	afs_filer_handler_t fsync;
	afs_filer_handler_t replicate;
};

static int filer_empty_process(struct afs_ctx *ctx,
				struct afs_filer_request *req)
{
	return 0;
}

/**
 * filer: none
 *
 * no striping, data of a file goes to one device. the device index is
 * statically determined by (ino % ndev).
 */

struct filer_none_data {
	int status;
	pthread_cond_t cond;
	pthread_mutex_t lock;
};

//static struct filer_none_data none;

/**
 * note that this function is executed by another thread.
 */
static void filer_none_callback(int status, struct afs_osd_request *r)
{
	struct filer_none_data *p = (struct filer_none_data *) r->priv;

	p->status = status;
	pthread_cond_signal(&p->cond);
}

static int filer_none_process_create(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	int ret;
	struct afs_osd_request or;
	struct afs_osd *osd = afs_osd(ctx);
	struct filer_none_data none;
	struct afs_stripe *sinfo = req->stripe;

	or.id = afs_now();
	or.type = AFS_OSD_RQ_CREATE;
	or.dev = sinfo->stloc < 0 ? req->ino % osd->ndev : sinfo->stloc;
	or.ino = req->ino;
	or.priv = &none;
	or.callback = filer_none_callback;

	none.status = 0;
	pthread_cond_init(&none.cond, NULL);
	pthread_mutex_init(&none.lock, NULL);

	ret = afs_osd_submit_request(osd, &or);
	if (ret)
		return ret;

	pthread_mutex_lock(&none.lock);
	ret = pthread_cond_wait(&none.cond, &none.lock);
	pthread_mutex_unlock(&none.lock);

	/** XXX: what do we need to do about the return from cond_wait? */

	return none.status;
}

static int filer_none_process_read(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	int ret;
	struct afs_osd_request or;
	struct afs_osd *osd = afs_osd(ctx);
	struct filer_none_data none;
	struct afs_stripe *sinfo = req->stripe;

	memset(&or, 0, sizeof(or));

	or.id = afs_now();
	or.type = AFS_OSD_RQ_READ;
	or.dev = sinfo->stloc < 0 ? req->ino % osd->ndev : sinfo->stloc;
	or.ino = req->ino;
	or.buf = req->buf;
	or.size = req->size;
	or.off = req->off;
	or.priv = &none;
	or.callback = filer_none_callback;

	none.status = 0;
	pthread_cond_init(&none.cond, NULL);
	pthread_mutex_init(&none.lock, NULL);

	ret = afs_osd_submit_request(osd, &or);
	if (ret)
		return ret;

	pthread_mutex_lock(&none.lock);
	ret = pthread_cond_wait(&none.cond, &none.lock);
	pthread_mutex_unlock(&none.lock);

	/** XXX: what do we need to do about the return from cond_wait? */

	return none.status;
}

static int filer_none_process_write(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	int ret;
	struct afs_osd_request or;
	struct afs_osd *osd = afs_osd(ctx);
	struct filer_none_data none;
	struct afs_stripe *sinfo = req->stripe;

	memset(&or, 0, sizeof(or));

	or.id = afs_now();
	or.type = AFS_OSD_RQ_WRITE;
	or.dev = sinfo->stloc < 0 ? req->ino % osd->ndev : sinfo->stloc;
	or.ino = req->ino;
	or.buf = req->buf;
	or.size = req->size;
	or.off = req->off;
	or.priv = &none;
	or.callback = filer_none_callback;

	none.status = 0;
	pthread_cond_init(&none.cond, NULL);
	pthread_mutex_init(&none.lock, NULL);

	ret = afs_osd_submit_request(osd, &or);
	if (ret)
		return ret;

	pthread_mutex_lock(&none.lock);
	ret = pthread_cond_wait(&none.cond, &none.lock);
	pthread_mutex_unlock(&none.lock);

	/** XXX: what do we need to do about the return from cond_wait? */

	return none.status;
}

static int filer_none_process_fsync(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	int ret;
	struct afs_osd_request or;
	struct afs_osd *osd = afs_osd(ctx);
	struct filer_none_data none;
	struct afs_stripe *sinfo = req->stripe;

	memset(&or, 0, sizeof(or));

	or.id = afs_now();
	or.type = AFS_OSD_RQ_DSYNC;
	or.dev = sinfo->stloc < 0 ? req->ino % osd->ndev: sinfo->stloc;
	or.ino = req->ino;
	or.priv = &none;
	or.callback = filer_none_callback;

	none.status = 0;
	pthread_cond_init(&none.cond, NULL);
	pthread_mutex_init(&none.lock, NULL);

	ret = afs_osd_submit_request(osd, &or);
	if (ret)
		return ret;

	pthread_mutex_lock(&none.lock);
	ret = pthread_cond_wait(&none.cond, &none.lock);
	pthread_mutex_unlock(&none.lock);

	return none.status;
}

static void filer_none_osd_replicate_callback(int status,
						struct afs_osd_request *r)
{
	struct afs_filer_request *req = r->priv;

	req->t_complete = afs_now();

	if (req->callback)
		(*req->callback) (status, (struct afs_filer_request *) req);
	free(r);
}

static int filer_none_process_replicate(struct afs_ctx *afs,
					struct afs_filer_request *req)
{
	int ret;
	struct afs_osd_request *or;
	struct afs_stripe *sinfo = req->stripe;

	or = malloc(sizeof(*or));
	if (!or)
		return -ENOMEM;

	memset(or, 0, sizeof(*or));
	or->id = afs_now();
	or->type = AFS_OSD_RQ_REPLICATE;
	or->ino = req->ino;
	or->dev = sinfo->stloc < 0 ?
			req->ino % afs_osd(afs)->ndev : sinfo->stloc;
	or->destdev = req->destdev;
	or->priv = req;
	or->callback = &filer_none_osd_replicate_callback;

	return afs_osd_submit_request(afs_osd(afs), or);
}

static struct afs_filer filer_none = {
	.name	= "none",
	.create	= filer_none_process_create,
	.read	= filer_none_process_read,
	.write	= filer_none_process_write,
	.fsync	= filer_none_process_fsync,
	.replicate = filer_none_process_replicate,
};

/**
 * filer: static
 *
 * static striping, data file is striped through available devices. application
 * can set desired stripe size and width for each file by setting its
 * attribute. if those are not set, the default setting (configured during
 * filesystem creation is used.
 */

static int filer_static_process_read(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	return 0;
}

static int filer_static_process_write(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	return 0;
}

static int filer_static_process_fsync(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	return 0;
}

static struct afs_filer filer_static = {
	.name	= "static",
	.create = NULL,
	.read	= filer_static_process_read,
	.write	= filer_static_process_write,
	.fsync	= filer_static_process_fsync,
	.replicate = NULL,
};

#if 0
/**
 * filer: jobaware
 *
 * jobaware data placement. the placement desicions are made by the job
 * scheduler.
 */

static int filer_jobaware_process_read(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	return 0;
}

static int filer_jobaware_process_write(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	return 0;
}

static int filer_jobaware_process_fsync(struct afs_ctx *ctx,
					struct afs_filer_request *req)
{
	return 0;
}


static struct afs_filer filer_static = {
	.name	= "jobaware",
	.read	= filer_jobaware_process_read,
	.write	= filer_jobaware_process_write,
	.fsync	= filer_jobaware_process_fsync,
};
#endif

/**
 * virtio
 */

#include "virtio.h"

static struct afs_filer filer_virtio = {
	.name	= "virtio",
	.create = NULL,
	.read	= afs_virtio_process_read,
	.write	= afs_virtio_process_write,
	.fsync	= NULL,
	.replicate = NULL,
};

/**
 * generic handlers.
 */

static struct afs_filer *filers[] = {
	&filer_none, &filer_virtio, &filer_static,
	/*&filer_jobaware,*/
};

int afs_filer_handle_create(struct afs_ctx *ctx,
				struct afs_filer_request *req)
{
	struct afs_filer *filer;
	struct afs_stripe *stripe;

	if (!ctx || !req)
		return -EINVAL;
	stripe = req->stripe;

	if (stripe->stmode < 0 || stripe->stmode >= AFS_N_STRIPE_MODES)
		return -EINVAL;

	filer = filers[stripe->stmode];

	if (filer->create)
		return filer->create(ctx, req);
	else
		return 0;	/** not ENOSYS here */
}

int afs_filer_handle_read(struct afs_ctx *ctx, struct afs_filer_request *req)
{
	struct afs_filer *filer;
	struct afs_stripe *stripe;

	if (!ctx || !req)
		return -EINVAL;
	stripe = req->stripe;

	if (stripe->stmode < 0 || stripe->stmode >= AFS_N_STRIPE_MODES)
		return -EINVAL;

	filer = filers[stripe->stmode];

	return filer->read(ctx, req);
}

int afs_filer_handle_write(struct afs_ctx *ctx, struct afs_filer_request *req)
{
	struct afs_filer *filer;
	struct afs_stripe *stripe;

	if (!ctx || !req)
		return -EINVAL;
	stripe = req->stripe;

	if (stripe->stmode < 0 || stripe->stmode >= AFS_N_STRIPE_MODES)
		return -EINVAL;

	filer = filers[stripe->stmode];

	return filer->write(ctx, req);
}

int afs_filer_handle_fsync(struct afs_ctx *ctx, struct afs_filer_request *req)
{
	struct afs_filer *filer;
	struct afs_stripe *stripe;

	if (!ctx || !req)
		return -EINVAL;

	stripe = req->stripe;
	filer = filers[stripe->stmode];

	if (filer->fsync)
		return filer->fsync(ctx, req);
	else
		return -ENOSYS;
}

int afs_filer_handle_replication(struct afs_ctx *ctx,
				struct afs_filer_request *req)
{
	struct afs_filer *filer;
	struct afs_stripe *stripe;

	if (!ctx || !req)
		return -EINVAL;

	req->t_submit = afs_now();

	stripe = req->stripe;
	filer = filers[stripe->stmode];

	if (filer->replicate)
		return filer->replicate(ctx, req);
	else
		return -ENOSYS;
}

