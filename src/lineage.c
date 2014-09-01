/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * lineage manager implementation.
 */
#include "activefs.h"

/**
 * external interface
 */

int afs_lineage_scan_reuse(struct afs_ctx *afs, struct afs_task *task)
{


	return 0;
}

int afs_lineage_process_read(struct afs_ctx *ctx,
				struct afs_filer_request *req)
{
	return ENOSYS;
}

int afs_lineage_process_write(struct afs_ctx *ctx,
				struct afs_filer_request *req)
{
	return ENOSYS;
}

int afs_lineage_record_job_execution(struct afs_ctx *ctx, struct afs_job *job)
{
	return 0;
}

