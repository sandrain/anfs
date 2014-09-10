/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * meta-scheduler implementation.
 */
#ifndef	__ANFS_SCHED_H__
#define	__ANFS_SCHED_H__

#include <stdio.h>
#include <pthread.h>
#include "parser.h"

struct anfs_task;
struct anfs_job;

/**
 * data availability.
 */
enum {
	ANFS_SCHED_DATA_NONE	= 0,	/** data is not available */
	ANFS_SCHED_DATA_PRODUCED,	/** data have just been produced */
	ANFS_SCHED_DATA_REUSE,		/** can reuse old one from lineage */
};

/**
 * because we are using the exofs backend by default, we have to keep track of
 * following information about data files.
 *  - anfs pathname
 *  - anfs ino
 *  - osd object id (exofs ino can be calculated from this: anfs_o2i(oid))
 */

struct anfs_data_file {
	int available;
	uint64_t ino;			/* anfs inode */
	uint64_t oid;			/* osd object id */
	int osd;			/* osd index */
	uint64_t size;
	struct anfs_task *producer;
	uint64_t parent;
	const char *path;		/* anfs pathname */
};

struct anfs_task_data {
	uint32_t n_files;
	struct anfs_data_file *files[0];
};

/**
 * the states for advancing task in fsm.
 */
enum {
	ANFS_SCHED_TASK_INIT	= 0,	/* task has been initialized */
	ANFS_SCHED_TASK_BLOCKED,	/* needs some input to be produced */
	ANFS_SCHED_TASK_WAITIO,		/* waiting for data transferring */
	ANFS_SCHED_TASK_AVAIL,		/* all task inputs are available */
	ANFS_SCHED_TASK_READY,		/* task is ready to run */
	ANFS_SCHED_TASK_SKIP,		/* lineage hit, no need to run */
	ANFS_SCHED_TASK_RUNNING,	/* task is currently running */
	ANFS_SCHED_TASK_COMPLETE,	/* task finished successfully */
	ANFS_SCHED_TASK_ABORT,		/* task aborted due to errors */
	ANFS_SCHED_TASK_ABANDONED,	/* task abandoned due to job abort */
};

struct anfs_task {
	struct anfs_job *job;
	const char *name;
	const char *kernel;
	char *argument;
	struct list_head list;	/** task list in the anfs_job */

	int status;		/** task status (ANFS_SCHED_TASK_..) */
	int ret;		/** exit status after execution */

	pthread_mutex_t stlock;	/** status lock (io_inflight, status) */
	int io_inflight;	/** in flight io counter */

	uint64_t input_cid;	/** collection id for input files */
	struct anfs_task_data *input;

	uint64_t output_cid;	/** collection id for output files */
	struct anfs_task_data *output;

	uint64_t tid;
	uint64_t koid;		/* kernel osd object id */
	uint64_t kino;		/* kernel ino in anfs */

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

struct anfs_job {
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
	ANFS_SCHED_BIND_LAZY	= -1,

	ANFS_SCHED_POLICY_RR	= 0,	/** default, round-robin */
	ANFS_SCHED_POLICY_INPUT	= 1,	/** input locality */
	ANFS_SCHED_POLICY_MINWAIT = 2,	/** minimum wait--greedy policy */

	ANFS_SCHED_N_POLICIES,
};

struct anfs_sched {
	/** scheduling policy */
	int policy;

	/** workers */
	pthread_t w_preparer;
	pthread_t w_advancer;
};

/**
 * logging
 */
static inline int anfs_job_log_open(struct anfs_job *job)
{
	FILE *fp;
	//char nbuf[128];

	if (!job)
		return -EINVAL;

	//sprintf(nbuf, "/tmp/afsjobs/%llu", anfs_llu(job->id));
	fp = fopen("/tmp/afsjobs/current", "a");

	if (!fp)
		return -errno;

	setlinebuf(fp);
	job->log = fp;

	return 0;
}

static inline void anfs_job_log_close(struct anfs_job *job)
{
	if (job && job->log)
		fclose(job->log);
}

#define	anfs_job_log(job, format, args...)			\
	fprintf(job->log, "[%llu] " format, anfs_llu(anfs_now()), ## args)
#define	anfs_task_log(task, format, args...)			\
	fprintf(task->job->log, "[%llu] %s (%p): " format,	\
			anfs_llu(anfs_now()), task->name, task, ## args)
#define anfs_job_report(job, format, args...)			\
	fprintf(job->log, format, ## args)

static inline void anfs_sched_dump_task_data(struct anfs_task_data *td,
						FILE *st)
{
	int i;
	struct anfs_data_file *current;
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

static inline void anfs_sched_dump_task(struct anfs_task *task, FILE *st)
{
	const char *pre = "  ";

	fprintf(st, "task [%s]\n", task->name);
	fprintf(st, "%skernel = %s\n", pre, task->kernel);
	fprintf(st, "%sargument = %s\n", pre, task->argument);
	fprintf(st, "%sinput:\n", pre);
	anfs_sched_dump_task_data(task->input, st);
	fprintf(st, "%soutput:\n", pre);
	anfs_sched_dump_task_data(task->output, st);
	fprintf(st, "\n");
}

static inline void anfs_sched_dump_job(struct anfs_job *job, FILE *st)
{
	struct anfs_task *t;

	fprintf(st, "JOB %s (%u tasks)======\n\n", job->name, job->n_tasks);

	list_for_each_entry(t, &job->task_list, list) {
		anfs_sched_dump_task(t, st);
	}
}

#define	anfs_job_log_dump(job)	anfs_sched_dump_job(job, (job)->log)


/**
 * external interface
 */

int anfs_sched_init(struct anfs_sched *self);

void anfs_sched_exit(struct anfs_sched *self);

int anfs_sched_submit_job(struct anfs_sched *self, const uint64_t ino);

#endif	/** __ANFS_SCHED_H__ */

