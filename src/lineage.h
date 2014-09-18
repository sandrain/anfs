/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * lineage/provenance manager.
 */
#ifndef	__AFS_LINEAGE_H__
#define	__AFS_LINEAGE_H__

struct anfs_ctx;
struct anfs_task;
struct anfs_job;

struct anfs_lineage {
};

/**
 * 
 *
 * @afs
 * @task
 *
 * 
 */
static inline int
anfs_lineage_scan_reuse(struct anfs_ctx *afs, struct anfs_task *task)
{
	return 0;
}


#if 0
int anfs_lineage_process_read(struct anfs_ctx *ctx,
				struct anfs_filer_request *req);

int anfs_lineage_process_write(struct anfs_ctx *ctx,
				struct anfs_filer_request *req);
#endif

static inline
int anfs_lineage_record_job_execution(struct anfs_ctx *ctx,
					struct anfs_job *job)
{
	return 0;
}

int anfs_lineage_get_task_runtime(struct anfs_ctx *ctx,
				struct anfs_task *task, double *runtime);

#endif	/** __AFS_LINEAGE_H__ */

