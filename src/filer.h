/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * interface of filer, which is responsible for end user data placement. this
 * is a sub component of afs_ctx, meaning that it doesn't have its own context.
 */
#ifndef	__AFS_FILER_H__
#define	__AFS_FILER_H__

struct afs_filer_request;
struct afs_task;

typedef void (*afs_filer_request_callback_t) (int status,
					struct afs_filer_request *r);

struct afs_filer_request {
	uint64_t ino;		/* inode number */
	void *buf;		/* data buffer */
	size_t size;		/* sizes in bytes */
	off_t off;		/* offset in the file */
	struct afs_stripe *stripe; /* placement policy */

	int destdev;		/* for creating a replication */
	void *priv;
	struct afs_task *task;
	afs_filer_request_callback_t callback;

	uint64_t t_submit;	/** currently only used for replication */
	uint64_t t_complete;
};

struct afs_ctx;

/**
 * afs_filer_handle_create creates an empty object. this is necessary if
 * osd collection is used.
 *
 * @ctx: afs_ctx
 * @req: request structure.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_filer_handle_create(struct afs_ctx *ctx,
				struct afs_filer_request *req);

/**
 * afs_filer_handle_read processes a file read operation.
 *
 * @ctx: afs_ctx
 * @req: request structure
 *
 * returns 0 on success, negatives on errors.
 */
int afs_filer_handle_read(struct afs_ctx *ctx, struct afs_filer_request *req);

/**
 * afs_filer_handle_write processes a file write operation.
 *
 * @ctx: afs_ctx
 * @req: request structure
 *
 * returns 0 on success, negatives on errors.
 */
int afs_filer_handle_write(struct afs_ctx *ctx, struct afs_filer_request *req);

/**
 * afs_filer_handle_fsync processes a fsync operation.
 *
 * @ctx: afs_ctx
 * @req: request structure. only stripe and ino fields are interested.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_filer_handle_fsync(struct afs_ctx *ctx, struct afs_filer_request *req);

int afs_filer_handle_replication(struct afs_ctx *ctx,
				struct afs_filer_request *req);

#endif	/** __AFS_FILER_H__ */

