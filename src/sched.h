/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * meta-scheduler implementation.
 */
#ifndef	__AFS_SCHED_H__
#define	__AFS_SCHED_H__

#include <stdio.h>
#include <pthread.h>
#include "parser.h"

struct afs_task;
struct afs_job;

/**
 * data availability.
 */
enum {
	AFS_SCHED_DATA_NONE	= 0,	/** data is not available */
	AFS_SCHED_DATA_PRODUCED,	/** data have just been produced */
	AFS_SCHED_DATA_REUSE,		/** can reuse old one from lineage */
};

struct afs_data_file {
	int available;
	uint64_t ino;
	int osd;
	uint64_t size;
	struct afs_task *producer;
	uint64_t parent;
	const char *path;
};

struct afs_task_data {
	uint32_t n_files;
	struct afs_data_file *files[0];
};

/**
 * the states for advancing task in fsm.
 */
enum {
	AFS_SCHED_TASK_INIT	= 0,	/* task has been initialized */
	AFS_SCHED_TASK_BLOCKED,		/* needs some input to be produced */
	AFS_SCHED_TASK_WAITIO,		/* waiting for data transferring */
	AFS_SCHED_TASK_AVAIL,		/* all task inputs are available */
	AFS_SCHED_TASK_READY,		/* task is ready to run */
	AFS_SCHED_TASK_SKIP,		/* lineage hit, no need to run */
	AFS_SCHED_TASK_RUNNING,		/* task is currently running */
	AFS_SCHED_TASK_COMPLETE,	/* task finished successfully */
	AFS_SCHED_TASK_ABORT,		/* task aborted due to errors */
	AFS_SCHED_TASK_ABANDONED,	/* task abandoned due to job abort */
};

struct afs_task {
	struct afs_job *job;
	const char *name;
	const char *kernel;
	char *argument;
	struct list_head list;	/** task list in the afs_job */

	int status;		/** task status (AFS_SCHED_TASK_..) */
	int ret;		/** exit status after execution */

	pthread_mutex_t stlock;	/** status lock (io_inflight, status) */
	int io_inflight;	/** in flight io counter */

	uint64_t input_cid;	/** collection id for input files */
	struct afs_task_data *input;

	uint64_t output_cid;	/** collection id for output files */
	struct afs_task_data *output;

	uint64_t tid;
	uint64_t koid;

	int affinity;		/** user-specified osd affinity,
				 * -1 if not set. */
	int osd;		/** which osd runs this task? */
	int q;			/** q currently linked */
	struct list_head tqlink; /** link for task queue */

	double	mw_submit;

	/**
	 * statistics
	 */
	uint64_t t_submit;
	uint64_t t_start;
	uint64_t t_complete;
	uint64_t t_transfer;
	uint64_t n_transfers;		/** # of file transfers. */
	uint64_t bytes_transfers;	/** bytes transfered */
};

struct afs_job {
	uint64_t id;
	const char *name;

	int status;
	int ret;

	uint32_t n_tasks;
	struct list_head list;
	struct list_head task_list;
	int sched;		/** scheduling policy */

	uint64_t t_submit;
	uint64_t t_start;
	uint64_t t_complete;

	FILE *log;		/** log stream */
};

enum {
	AFS_SCHED_BIND_LAZY	= -1,

	AFS_SCHED_POLICY_RR	= 0,	/** default, round-robin */
	AFS_SCHED_POLICY_INPUT	= 1,	/** input locality */
	AFS_SCHED_POLICY_MINWAIT = 2,	/** minimum wait--greedy policy */

	AFS_SCHED_N_POLICIES,
};

struct afs_sched {
	/** scheduling policy */
	int policy;

	/** workers */
	pthread_t w_preparer;
	pthread_t w_advancer;
};

/**
 * logging
 */
static inline int afs_job_log_open(struct afs_job *job)
{
	FILE *fp;
	char nbuf[128];

	if (!job)
		return -EINVAL;

	sprintf(nbuf, "/tmp/afsjobs/%llu", afs_llu(job->id));
	fp = fopen(nbuf, "w");

	if (!fp)
		return -errno;

	setlinebuf(fp);
	job->log = fp;

	return 0;
}

static inline void afs_job_log_close(struct afs_job *job)
{
	if (job && job->log)
		fclose(job->log);
}

#define	afs_job_log(job, format, args...)			\
	fprintf(job->log, "[%llu] " format, afs_llu(afs_now()), ## args)
#define	afs_task_log(task, format, args...)			\
	fprintf(task->job->log, "[%llu] %s (%p): " format,	\
			afs_llu(afs_now()), task->name, task, ## args)
#define afs_job_report(job, format, args...)			\
	fprintf(job->log, format, ## args)

static inline void afs_sched_dump_task_data(struct afs_task_data *td, FILE *st)
{
	int i;
	struct afs_data_file *current;
	const char *pre = "    ";

	fprintf(st, "%s%d files: \n", pre, td->n_files);
	for (i = 0; i < td->n_files; i++) {
		current = td->files[i];
		fprintf(st, "%s - %s ", pre, current->path);
		if (current->producer)
			fprintf(st, "produced by [%s]\n",
				current->producer->name);
		else
			fprintf(st, "\n");
	}
}

static inline void afs_sched_dump_task(struct afs_task *task, FILE *st)
{
	const char *pre = "  ";

	fprintf(st, "task [%s]\n", task->name);
	fprintf(st, "%skernel = %s\n", pre, task->kernel);
	fprintf(st, "%sargument = %s\n", pre, task->argument);
	fprintf(st, "%sinput:\n", pre);
	afs_sched_dump_task_data(task->input, st);
	fprintf(st, "%soutput:\n", pre);
	afs_sched_dump_task_data(task->output, st);
	fprintf(st, "\n");
}

static inline void afs_sched_dump_job(struct afs_job *job, FILE *st)
{
	struct afs_task *t;

	fprintf(st, "JOB %s (%u tasks)======\n\n", job->name, job->n_tasks);

	list_for_each_entry(t, &job->task_list, list) {
		afs_sched_dump_task(t, st);
	}
}

#define	afs_job_log_dump(job)	afs_sched_dump_job(job, (job)->log)


/**
 * external interface
 */

/**
 * 
 *
 * @self
 *
 * 
 */
int afs_sched_init(struct afs_sched *self);

/**
 * 
 *
 * @self
 */
void afs_sched_exit(struct afs_sched *self);

/**
 * 
 *
 * @self
 * @ino
 *
 * 
 */
int afs_sched_submit_job(struct afs_sched *self, const uint64_t ino);

#endif	/** __AFS_SCHED_H__ */

