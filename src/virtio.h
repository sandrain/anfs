/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * virtual entry management.
 */
#ifndef	__AFS_VIRTIO_H__
#define	__AFS_VIRTIO_H__

struct afs_job;

struct afs_virtio {
	uint64_t i_jobs;
	uint64_t i_failed;
	uint64_t i_running;
	uint64_t i_submit;
};

/**
 * 
 *
 * @self
 *
 * 
 */
int afs_virtio_init(struct afs_virtio *self);

/**
 * 
 *
 * @self
 */
void afs_virtio_exit(struct afs_virtio *self);

/**
 * 
 *
 * @self
 * @ino
 *
 * returns 0 if it's not virtual,
 *	   1 if it's a virtual entry,
 *	   2 if it's a virtual root entry,
 *	   negatives on errors.
 */
int afs_virtio_is_virtual(struct afs_virtio *self, uint64_t ino);

/**
 * 
 *
 * @ctx
 * @req
 *
 * 
 */
int afs_virtio_process_read(struct afs_ctx *ctx,
				struct afs_filer_request *req);

/**
 * 
 *
 * @ctx
 * @req
 *
 */
int afs_virtio_process_write(struct afs_ctx *ctx,
				struct afs_filer_request *req);

int afs_virtio_create_job_entry(struct afs_virtio *self, struct afs_job *job);

#endif	/** __AFS_VIRTIO_H__ */

