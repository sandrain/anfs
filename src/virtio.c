/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * implementation of virtual entries in activefs.
 */
#include "activefs.h"

static inline mode_t afs_default_mode_virtual_files_ro(void)
{
	return (mode_t) 0 | (S_IFREG | S_IRUSR);
}

static inline mode_t afs_default_mode_virtual_files_wo(void)
{
	return (mode_t) 0 | (S_IFREG | S_IWUSR);
}

static inline mode_t afs_default_mode_virtual_files_rw(void)
{
	return (mode_t) 0 | (S_IFREG | S_IWUSR | S_IRUSR);
}

static inline mode_t afs_default_mode_virtual_dirs(void)
{
	return (mode_t) 0 | (S_IFDIR | S_IRUSR | S_IXUSR);
}

#if 0
/** isn't there a way to avoid this?? */
static int find_inode_from_path(struct afs_ctx *ctx, const char *path,
			uint64_t *ino)
{
}
#endif

/**
 * External interface
 */

int afs_virtio_init(struct afs_virtio *self)
{
	struct afs_ctx *ctx = afs_ctx(self, virtio);

	self->i_jobs = afs_super(ctx)->i_jobs;
	self->i_failed = afs_super(ctx)->i_failed;
	self->i_running = afs_super(ctx)->i_running;
	self->i_submit = afs_super(ctx)->i_submit;

	return 0;
}

void afs_virtio_exit(struct afs_virtio *self)
{
}

int afs_virtio_is_virtual(struct afs_virtio *self, uint64_t ino)
{
	struct afs_ctx *ctx = afs_ctx(self, virtio);

	if (ino == self->i_jobs)
		return 2;
	else if (ino == self->i_failed)
		return 2;
	else if (ino == self->i_running)
		return 2;
#if 0
	else if (ino == self->submit) /** this should not happen */
		return 1;
#endif
	else
		return afs_db_is_virtual_inode(afs_db(ctx), ino);
}

/**
 * the filer functions. see filer.h.
 */
int afs_virtio_process_read(struct afs_ctx *ctx,
				struct afs_filer_request *req)
{
	return ENOSYS;
}

int afs_virtio_process_write(struct afs_ctx *ctx,
				struct afs_filer_request *req)
{
	int ret = 0;
	uint64_t ino;
	struct afs_virtio *self = afs_virtio(ctx);

	if (req->ino == self->i_submit) {
		ino = strtoull(req->buf, NULL, 10);
		ret = afs_sched_submit_job(afs_sched(ctx), ino);
		if (ret)
			goto out;
	}
	else
		ret = EINVAL;

out:
	return ret;
}

int afs_virtio_create_job_entry(struct afs_virtio *self, struct afs_job *job)
{
	int ret;

	return 0;
}

