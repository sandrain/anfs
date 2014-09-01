/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * lineage/provenance manager.
 */
#ifndef	__AFS_LINEAGE_H__
#define	__AFS_LINEAGE_H__

struct afs_task;

struct afs_lineage {
};

/**
 * 
 *
 * @afs
 * @task
 *
 * 
 */
int afs_lineage_scan_reuse(struct afs_ctx *afs, struct afs_task *task);

int afs_lineage_process_read(struct afs_ctx *ctx,
				struct afs_filer_request *req);

int afs_lineage_process_write(struct afs_ctx *ctx,
				struct afs_filer_request *req);

int afs_lineage_record_job_execution(struct afs_ctx *ctx, struct afs_job *job);

#endif	/** __AFS_LINEAGE_H__ */

