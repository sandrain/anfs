/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * lineage manager implementation.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "anfs.h"

/**
 * external interface
 */

#if 0
int anfs_lineage_scan_reuse(struct anfs_ctx *afs, struct anfs_task *task)
{
	return 0;
}

int anfs_lineage_process_read(struct anfs_ctx *ctx,
				struct anfs_filer_request *req)
{
	return ENOSYS;
}

int anfs_lineage_process_write(struct anfs_ctx *ctx,
				struct anfs_filer_request *req)
{
	return ENOSYS;
}

int anfs_lineage_record_job_execution(struct anfs_ctx *ctx, struct anfs_job *job)
{
	return 0;
}
#endif

struct anfs_runtime_record {
	double runtime;
	char name[32];
};

#define	ANFS_MAX_RUNTIME_RECORD		128
#define ANFS_RUNTIME_RECORD_FILE	\
	"/ccs/techint/proj/anFS/__experiments/ex2_schedule/anfs-runtime.txt"

static struct anfs_runtime_record records[ANFS_MAX_RUNTIME_RECORD];
static uint64_t n_records;

/** we only have a small number of records, just do the sequential search. */
int anfs_lineage_get_task_runtime(struct anfs_ctx *ctx,
				struct anfs_task *task, double *runtime)
{
	int ret = 0;
	const char *name = task->name;
	struct anfs_runtime_record *current = records;

	while (current->name[0] != '\0') {
		if (!strncmp(name, current->name, 31)) {
			*runtime = current->runtime;
			goto out;
		}

		current++;
	}

	ret = -1;
out:
	return ret;
}

/** use the constructor to read the records from the file */
__attribute__((constructor))
static void anfs_lineage_ctor(void)
{
	FILE *fp;
	char *pos;
	char linebuf[256];

	fp = fopen(ANFS_RUNTIME_RECORD_FILE, "r");

	while (fgets(linebuf, 256, fp) != NULL) {
		if (strempty(linebuf) || linebuf[0] == '#')
			continue;

		pos = strchr(linebuf, ':');
		if (pos == NULL)
			continue;
		*pos++ = '\0';

		strcpy(records[n_records].name, linebuf);
		sscanf(pos, "%lf", &records[n_records].runtime);

		n_records++;
	}

	fclose(fp);
}

