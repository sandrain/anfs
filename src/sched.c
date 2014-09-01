/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * the main scheduler of activefs.
 */
#include "activefs.h"

enum {
	Q_WAITING	= 0,
	Q_RUNNING,
	Q_COMPLETE,
	Q_LEN,
};

typedef	int (*afs_sched_func_t) (struct afs_ctx *, struct afs_job *);

/**
 * pathdb handling helpers
 */

static const uint64_t AFS_OBJECT_OFFSET = 0x10000;

static inline int pathdb_insert(struct afs_ctx *ctx, uint64_t ino)
{
	char path[2048];	/* should be enough for now */
	int ret = afs_db_get_full_path(afs_db(ctx), ino, path);

	return afs_pathdb_insert(afs_pathdb(ctx),
			(uint64_t) 0x22222, ino + AFS_OBJECT_OFFSET,
			path);
}

static inline int pathdb_update(struct afs_ctx *ctx, uint64_t ino)
{
	char path[2048];
	int ret = afs_db_get_full_path(afs_db(ctx), ino, path);

	return afs_pathdb_update(afs_pathdb(ctx),
			(uint64_t) 0x22222, ino + AFS_OBJECT_OFFSET,
			path);
}

static inline int pathdb_remove(struct afs_ctx *ctx, uint64_t ino)
{
	return afs_pathdb_remove(afs_pathdb(ctx),
			(uint64_t) 0x22222, ino + AFS_OBJECT_OFFSET);
}

/**
 * job id assignment
 * TODO: delegate this to sqlite.
 */
static uint64_t g_jobid = 1;
static pthread_mutex_t g_jobid_lock;

static inline uint64_t next_job_id(void)
{
	uint64_t id;

	pthread_mutex_lock(&g_jobid_lock);
	id = g_jobid++;
	pthread_mutex_unlock(&g_jobid_lock);

	return id;
}

/**
 * job queue
 */
static pthread_mutex_t jql[Q_LEN];	/** per q lock */
static struct list_head jq[Q_LEN];

/**
 * task queue
 */
static pthread_mutex_t tql[Q_LEN];	/** per q lock */
static struct list_head tq[Q_LEN];

static inline int jq_lock(int q)
{
	return pthread_mutex_lock(&jql[q]);
}

static inline int jq_unlock(int q)
{
	return pthread_mutex_unlock(&jql[q]);
}

static inline void jq_append(struct afs_job *job, int q)
{
	switch (q) {
	case Q_WAITING: job->t_submit = afs_now(); break;
	case Q_RUNNING: job->t_start = afs_now(); break;
	case Q_COMPLETE: job->t_complete = afs_now(); break;
	default: return;
	}

	list_add_tail(&job->list, &jq[q]);
}

static inline struct afs_job *jq_fetch(int q)
{
	struct afs_job *job;

	if (list_empty(&jq[q]))
		return NULL;

	job = list_first_entry(&jq[q], struct afs_job, list);
	list_del(&job->list);

	return job;
}

static inline int tq_lock(int q)
{
	return pthread_mutex_lock(&tql[q]);
}

static inline int tq_unlock(int q)
{
	return pthread_mutex_unlock(&tql[q]);
}

static inline void tq_append(struct afs_task *task, int q)
{
#if 0
	switch (q) {
	case Q_WAITING: task->t_submit = afs_now(); break;
	case Q_RUNNING: task->t_start = afs_now(); break;
	case Q_COMPLETE: task->t_complete = afs_now(); break;
	default: return;
	}
#endif

	task->q = q;
	list_add_tail(&task->tqlink, &tq[q]);
}

static inline struct afs_task *tq_fetch(int q)
{
	struct afs_task *task;

	if (list_empty(&tq[q]))
		return NULL;

	task = list_first_entry(&tq[q], struct afs_task, tqlink);
	list_del(&task->tqlink);

	return task;
}

static int afs_sched_rr(struct afs_ctx *afs, struct afs_job *job);
static int afs_sched_input(struct afs_ctx *afs, struct afs_job *job);

/**
 * find_inode returns inode # of the given @path, which should be rooted by the
 * mounting point.
 *
 * @afs: afs_ctx.
 * @path: path rooted by the mounting point.
 *
 * returns inode # if found, 0 otherwise.
 */

#if 0
static inline uint64_t find_inode(struct afs_ctx *afs, const char *path,
				int *osd)
{
	uint64_t ino;
	struct afs_stripe sinfo;
	int ret = afs_db_find_inode_from_path(afs_db(afs), path, &ino);

	if (!osd)
		return ret ? 0 : ino;

	ret = afs_db_get_stripe_info(afs_db(afs), ino, &sinfo);
	if (ret)
		return 0;

	osd = sinfo.stloc;
	return ino;
}
#endif

static uint64_t find_inode(struct afs_ctx *afs, const char *path,
			struct afs_data_file *file)
{
	int ret;
	uint64_t ino;
	struct stat stbuf;
	struct afs_stripe sinfo;

	ret = afs_db_find_inode_from_path(afs_db(afs), path, &stbuf);
	if (!file)
		return ret ? 0 : stbuf.st_ino;
	ino = stbuf.st_ino;

	ret = afs_db_get_stripe_info(afs_db(afs), ino, &sinfo);
	if (ret)
		return 0;

	file->ino = ino;
	file->osd = sinfo.stloc < 0 ? ino % afs_osd(afs)->ndev : sinfo.stloc;
	file->size = stbuf.st_size;

	return ino;
}

static int validate_data_files(struct afs_ctx *afs, struct afs_job *job)
{
	int i, count;
	uint64_t ino;
	struct afs_task *t;
	struct afs_task_data *td;
	struct afs_data_file *df;

	list_for_each_entry(t, &job->task_list, list) {
		if (!t)
			continue;

		ino = find_inode(afs, t->kernel, NULL);
		if (!ino)
			return -EINVAL;
		t->koid = ino;

		td = t->input;
		count = td->n_files;

		for (i = 0; i < count; i++) {
			df = td->files[i];

			/**
			 * files without producers should exist before running
			 * the job.  files with producers are checked in the
			 * next loop.
			 */
			if (df->producer == NULL) {
				ino = find_inode(afs, df->path, df);
				if (!ino)
					return -EINVAL;

				df->available = 1;
			}
		}

		td = t->output;
		count = td->n_files;

		for (i = 0; i < count; i++) {
			df = td->files[i];

			ino = find_inode(afs, df->path, df);
			if (!ino)
				return -EINVAL;

			df->available = 0;
		}
	}

	return 0;
}

static afs_sched_func_t sched_funcs[] = 
{
	&afs_sched_rr,
	&afs_sched_input,
	&afs_sched_input,	/** minwait */
};

static inline int schedule(struct afs_ctx *afs, struct afs_job *job)
{
#if 0
	int policy = afs_sched(afs)->policy;
#endif
	int policy = job->sched;

	afs_sched_func_t func = sched_funcs[policy];
	return (*func) (afs, job);
}

/**
 * osd_task_complete_callback just change the task status and put rest of the
 * things to the advancer.
 *
 * @status: task return code (0 on success).
 * @arg: the task descriptor.
 */
static void osd_task_complete_callback(int status, struct afs_osd_request *r)
{
	uint32_t i;
	struct afs_task *t = r->task;

	tq_lock(Q_RUNNING);
	list_del(&t->tqlink);
	tq_unlock(Q_RUNNING);

	t->ret = status;

afs_task_log(t, "task execution complete from osd %d (tid = %llu, ret = %d)\n",
		t->osd, afs_llu(t->tid), status);

	if (status)
		t->status = AFS_SCHED_TASK_ABORT;
	else {
		t->status = AFS_SCHED_TASK_COMPLETE;

		/** 
		 * handling output files:
		 * . mark that they are available
		 * . adjust the size by checking with osd
		 * . set the location of the file in inode
		 */
		for (i = 0; i < t->output->n_files; i++) {
			int ret = 0;
			uint64_t size;
			struct afs_ctx *afs = (struct afs_ctx *) r->priv;
			struct afs_data_file *file = t->output->files[i];

			ret = afs_osd_get_file_size(afs_osd(afs), r->dev,
						0x22222, file->ino, &size);
			if (ret) {
				/** 
				 * XXX: is this correct?
				 */
				t->status = AFS_SCHED_TASK_ABORT;
				goto out;
			}

			ret = afs_db_update_task_output_file(afs_db(afs),
						file->ino, t->osd, size);
			file->osd = t->osd;	/** update current position */
			file->size = size;

afs_task_log(t, "update output file metadata (ino=%llu, dev=%d, size=%llu)"
		", (ret = %d)\n",
		 afs_llu(file->ino), t->osd, afs_llu(size), ret);

			if (ret) {
				t->status = AFS_SCHED_TASK_ABORT;
				goto out;
			}

			ret = afs_db_invalidate_replica(afs_db(afs),
							file->ino);

afs_task_log(t, "invalidate replicas for output file (ino=%llu)"
		", (ret = %d)\n", afs_llu(file->ino), ret);

			if (ret) {
				t->status = AFS_SCHED_TASK_ABORT;
				goto out;
			}

			file->available = 1;
		}
	}

	/**
	 * TODO: we need to check if the whole job is finished.
	 */

out:
	tq_lock(Q_WAITING);
	tq_append(t, Q_WAITING);
	tq_unlock(Q_WAITING);

	free(r);
}

/**
 * should be done:
 * . create collections for input and output objects.
 */
static int prepare_collections(struct afs_ctx *afs, struct afs_task *t)
{
	int ret;
	uint64_t cids[2];
	struct afs_osd *afs_osd = afs_osd(afs);
	struct afs_job *job = t->job;

	ret = afs_db_assign_collection_ids(afs_db(afs), 2, cids);
	if (ret)
		return ret;

	ret = afs_osd_create_collection(afs_osd, t->osd, 0x22222, &cids[0]);
	ret |= afs_osd_create_collection(afs_osd, t->osd, 0x22222, &cids[1]);

afs_task_log(t, "input/output collections create (ret=%d).\n", ret);

	if (!ret) {
		t->input_cid = cids[0];
		t->output_cid = cids[1];
	}

	return ret;
}

static int fsm_request_task_execution(struct afs_ctx *afs, struct afs_task *t)
{
	int ret;
	struct afs_osd_request *r;
	uint32_t i;
	//uint64_t *inobjs, *outobjs;
	uint32_t n_ins = t->input->n_files;
	uint32_t n_outs = t->output->n_files;
	struct afs_data_file *file;

	/** create collections */
	ret = prepare_collections(afs, t);
	if (ret)
		return ret;

#if 0
	/** associate objects to collections. */
	inobjs = malloc(sizeof(*inobjs) * (n_ins + n_outs));
	if (!inobjs)
		return -ENOMEM;

	outobjs = &inobjs[n_ins];

	for (i = 0; i < t->input->n_files; i++) {
		file = t->input->files[i];
		inobjs[i] = file->ino;
	}

	for (i = 0; i < t->output->n_files; i++) {
		file = t->output->files[i];
		outobjs[i] = file->ino;
	}

	ret = afs_osd_set_membership(afs_osd(afs), t->osd, 0x22222,
				t->input_cid, inobjs, n_ins);
afs_task_log(t, "membership set for input %d objects (ret=%d).\n",
		t->input->n_files, ret);
	if (ret)
		goto out_free_olist;
	ret = afs_osd_set_membership(afs_osd(afs), t->osd, 0x22222,
				t->output_cid, outobjs, n_outs);
afs_task_log(t, "membership set for output %d objects (ret=%d).\n",
		t->output->n_files, ret);
	if (ret)
		goto out_free_olist;
#endif

	/** prepare and submit request */
	r = calloc(1, sizeof(*r));
	if (!r) {
		ret = -ENOMEM;
		goto out_free_olist;
	}

	r->type = AFS_OSD_RQ_EXECUTE;
	r->dev = t->osd;
	r->task = t;
	r->priv = afs;
	r->callback = &osd_task_complete_callback;

	return afs_osd_submit_request(afs_osd(afs), r);

out_free_olist:
	//free(inobjs);
	return ret;
}


/**
 * thread workers
 */

/**
 * responsibilities:
 * (for each job fetched)
 * . check if we can reuse some data files (scan lineage info)
 * . append all tasklets into task queue
 * . create a virtual entry for the job
 */
static void *sched_worker_preparer(void *arg)
{
	int ret;
	struct afs_job *job;
	struct afs_task *t;
	struct afs_ctx *afs = (struct afs_ctx *) arg;

	while (1) {
		jq_lock(Q_WAITING);
		job = jq_fetch(Q_WAITING);
		jq_unlock(Q_WAITING);

		if (!job) {
			usleep(1000);
			continue;
		}

		/** XXX: assign osd to each task */
		ret = schedule(afs, job);

		list_for_each_entry(t, &job->task_list, list) {
			ret = afs_lineage_scan_reuse(afs, t);
			t->status = ret ?
				AFS_SCHED_TASK_SKIP : AFS_SCHED_TASK_INIT;

			//ret = prepare_collections(afs, t);

			pthread_mutex_init(&t->stlock, NULL);

			tq_lock(Q_WAITING);
			tq_append(t, Q_WAITING);
			tq_unlock(Q_WAITING);
		}

		jq_lock(Q_RUNNING);
		jq_append(job, Q_RUNNING);
		jq_unlock(Q_RUNNING);

		ret = afs_virtio_create_job_entry(afs_virtio(afs), job);
		if (!ret) {
			/** report error!! */
		}
	}

	return (void *) 0;
}

/**
 * finite state manchine to advance the job processing.
 *
 * responsibilities:
 * . fetch tasks in the wait list and assign them to osd.
 * . on completion of some jobs, fetch tasks who wait for the output.
 */

/**
 * advancer private routines (fsm_*)
 */

static inline struct afs_task *fsm_fetch(int q)
{
	struct afs_task *t;

	tq_lock(q);
	t = tq_fetch(q);
	tq_unlock(q);

	return t;
}

static inline void fsm_qtask(struct afs_task *t, int q)
{
	tq_lock(q);
	tq_append(t, q);
	tq_unlock(q);
}

static inline int fsm_task_input_produced(struct afs_task *t)
{
	int i;
	struct afs_task_data *input = t->input;

	/** check necessary files are all created. */
	for (i = 0; i < input->n_files; i++)
		if (!input->files[i]->available)
			return 0;
#if 0
	/** this pollutes log too much */
		else {
afs_task_log(t, "input %s is ready in osd %d.\n", input->files[i]->path,
			input->files[i]->osd);
		}
#endif

	/**
	 * if no input files specified, return as if they were produced
	 * already.
	 */

	return 1;
}

static void fsm_replication_callback(int status, struct afs_filer_request *req)
{
	int ret;
	struct afs_ctx *ctx = (struct afs_ctx *) req->priv;
	struct afs_task *t = req->task;
	struct afs_job *job = t->job;

	if (status == 0) {
		ret = afs_db_add_replication(afs_db(ctx), req->ino, req->destdev);
		if (ret) {
			/** XXX: what a mess! what should we do? */
afs_task_log(t, "file replication (%llu) seems to be failed (ret=%d) !!!!",
		afs_llu(req->ino), status);
		}

		t->t_transfer += req->t_complete - req->t_submit;
		t->n_transfers += 1;
		t->bytes_transfers += req->stripe->stat.st_size;
	}

	/** this field is continuously accessed/checked by the advancer */
	pthread_mutex_lock(&t->stlock);
	t->io_inflight--;
	if (t->io_inflight <= 0)
		req->task->status = AFS_SCHED_TASK_READY;
	pthread_mutex_unlock(&t->stlock);

afs_task_log(t, "data transfer finished for ino %llu (status=%d), "
		 "%d in-flight io presents\n",
		afs_llu(req->ino), status, t->io_inflight);

	free(req);
}

/**
 * returns:
 * 0 if no replication is required
 * 1 if replication is necessary, and the request structure has been
 * sucessfully initialized.
 * negatives on errors.
 */
static int get_replication_request(struct afs_ctx *afs, uint64_t oid, int tdev,
				int *dev_out, struct afs_filer_request **out_req)
{
	int ret = 0;
	int dev;
	struct afs_db *db = afs_db(afs);
	struct afs_stripe sinfo, *sp;
	struct afs_filer_request *req = NULL;

	ret = afs_db_get_stripe_info(db, oid, &sinfo);
	if (ret)
		return -EINVAL;

	if (sinfo.stloc < 0)
		dev = oid % afs_osd(afs)->ndev;
	else
		dev = sinfo.stloc;

	if (dev == tdev)
		return 0;		/** replication avail */

	ret = afs_db_replication_available(db, oid, tdev);
	if (ret < 0)
		return -EIO;
	else if (ret > 0)
		return 0;		/** replication avail */

	req = calloc(1, sizeof(*req) + sizeof(*sp));
	if (!req)
		return -ENOMEM;

	sp = (struct afs_stripe *) &req[1];
	*sp = sinfo;

	req->ino = oid;
	req->destdev = tdev;
	req->stripe = sp;
	req->priv = afs;
	/** req->task should be set by caller */
	req->callback = &fsm_replication_callback;

	if (dev_out)
		*dev_out = dev;
	*out_req = req;
	return 1;
}

static int sched_input_assign_osd(struct afs_ctx *afs, struct afs_task *t)
{
	uint32_t i;
	int max = 0;
	uint64_t *datapos, maxval = 0;
	struct afs_task_data *input;
	struct afs_data_file *file;

	datapos = afs_calloc(afs_osd(afs)->ndev, sizeof(uint64_t));

	input = t->input;
	for (i = 0; i < input->n_files; i++) {
		file = input->files[i];
		datapos[file->osd] += file->size;
	}

	for (i = 0; i < afs_osd(afs)->ndev; i++) {
afs_task_log(t, " -- osd[%d] = %llu bytes\n", i, afs_llu(datapos[i]));
		if (datapos[i] > maxval) {
			maxval = datapos[i];
			max = i;
		}
	}
afs_task_log(t, " -- ## task is scheduled to osd %d\n", max);

	free(datapos);

afs_task_log(t, "task is scheduled to osd %d\n", max);
	return max;
}

#if 1
/** minwait implementation.
 * minwait computes expected waiting time for a task before the task is
 * executed. This currently involves considering the following factors:
 *
 * - expected data movement cost
 * - device q wait time
 *
 * For the expected movement cost, we calculate it by ourselves. For the device
 * wait time, we need to query device. Currently, just to see the effects, we
 * open a shared file and each osd will update expected waiting time, which the
 * scheduler only accesses to read the values.
 *
 * XXX: do we need to use the double precision??
 */

#define	MINWAIT_MAXOSD		8

#if 0
static const uint64_t minwait_bw = 50*(1<<20);		/** assume 100 MB/s */
#endif
static const uint64_t minwait_bw = (1<<29);		/** assume 1 GB/s */

static double calculate_transfer_cost(struct afs_ctx *afs, struct afs_task *t,
					int osd)
{
	uint32_t i;
	uint64_t bytes = 0, wait;
	struct afs_task_data *input = t->input;
	struct afs_data_file *file;

	for (i = 0; i < input->n_files; i++) {
		file = input->files[i];
		if (file->osd == osd)	/** we don't count of its own */
			continue;

		bytes += file->size;
	}

	return (double) bytes * 2 / minwait_bw + 0.5 * input->n_files;
}

/**
 * For the fast prototype, a shared file for each device is statically named
 * after the hostname itself.
 */

#if 0
static const char *devq_dir = "/ccs/techint/home/hs2/afs_eval/devq/";
static const char *osd_hostnames[] = {
	"atom-a1", "atom-a2", "atom-b1", "atom-b2",
	"atom-c1", "atom-c2", "atom-d1", "atom-d2"
};

static FILE *dfps[8];

static double calculate_queue_cost(struct afs_ctx *afs, int osd)
{
	FILE *fp;
	char pathbuf[128];
	uint64_t wait;
	int random = rand();
	double rv = ((double) random / RAND_MAX);

	if (dfps[osd] == NULL) {
		sprintf(pathbuf, "%s%s", devq_dir, osd_hostnames[osd]);
		if ((fp = fopen(pathbuf, "r")) == NULL)
			return 5 + rv;
		else
			dfps[osd] = fp;
	}
	else
		fp = dfps[osd];

	if (1 != fread(&wait, sizeof(wait), 1, fp))
		wait = (uint64_t) 5;

	return wait + rv;
}
#endif

#endif

static double wait_time[MINWAIT_MAXOSD];
static double minwait_timestamp;

static inline void minwait_update_time(struct afs_ctx *afs)
{
	uint32_t i;
	uint64_t old = minwait_timestamp;
	struct timeval tv;

	gettimeofday(&tv, NULL);
	minwait_timestamp = tv.tv_sec + tv.tv_usec * 0.000001;

	for (i = 0; i < afs_osd(afs)->ndev; i++) {
		wait_time[i] -= minwait_timestamp - old;
		if (wait_time[i] < 0)
			wait_time[i] = 0;
	}
}

static inline double minwait_get_wait_time(struct afs_ctx *afs, int osd)
{
	minwait_update_time(afs);
	return wait_time[osd];
}

static inline double get_task_runtime(struct afs_task *t)
{
	const char *path = t->name;

	/** arbitrary variation to break the tie*/
	int r = rand();
	double ret, rv = (double) r / RAND_MAX / 2;

	if (strstr(path, "mImgtbl") != NULL)		/** montage kernels */
		ret = 1;
	else if (strstr(path, "mProjectPP") != NULL)
		ret = 5;
	else if (strstr(path, "mAdd") != NULL)
		ret = 5;
	else if (strstr(path, "mJPEG") != NULL)
		ret = 6;
	else if (strstr(path, "mOverlaps") != NULL)
		ret = 1;
	else if (strstr(path, "mDiffFit") != NULL)
		ret = 2;
	else if (strstr(path, "mBgModel") != NULL)
		ret = 1;
	else if (strstr(path, "mBgExec") != NULL)
		ret = 3;
	else
		ret = 0;

	return (ret + rv) * 2;
}

static int sched_minwait_assign_osd(struct afs_ctx *afs, struct afs_task *t)
{
	uint32_t i;
	int min = 0;
	double minval = (double) UINT64_MAX;
	double dt[MINWAIT_MAXOSD];
	double wait[MINWAIT_MAXOSD];

	memset(wait, 0x00, sizeof(uint64_t)*MINWAIT_MAXOSD);

	for (i = 0; i < afs_osd(afs)->ndev; i++) {
		dt[i] = calculate_transfer_cost(afs, t, i);
		wait[i] = dt[i];
		wait[i] += minwait_get_wait_time(afs, i);

afs_task_log(t, " -- osd[%d] = %lf seconds wait\n", i, wait[i]);

		if (wait[i] <= minval) {	/** whenever there is a tie, change */
			min = i;
			minval = wait[i];
		}
	}

	wait_time[min] += dt[min] + get_task_runtime(t);
	t->mw_submit = minwait_timestamp;

afs_task_log(t, " -- ## task submitted to osd %d\n", min);

	return min;
}

/**
 * FIXME: each scheduling policy has to implement function table.
 */
static inline void fsm_schedule_input_lazy(struct afs_ctx *afs,
						struct afs_task *t)
{
	int policy = t->job->sched;

	if (policy == AFS_SCHED_POLICY_RR)
		return;

	switch (policy) {
		case AFS_SCHED_POLICY_INPUT:
			t->osd = sched_input_assign_osd(afs, t);
			break;
		case AFS_SCHED_POLICY_MINWAIT:
			t->osd = sched_minwait_assign_osd(afs, t);
			break;
		default:
			break;
	}
}

/**
 * returns:
 * number of transfer requested.
 * 0 if no more transfer is required (task is ready for execution).
 * negatives on errors: caller should try again later.
 *
 * note that we also need to move the .so kernel file to the desired location.
 */
static int fsm_request_data_transfer(struct afs_ctx *afs, struct afs_task *t)
{
	int ret = 0;
	int i, count = 0;
	int dev, target_dev = t->osd;
	struct afs_task_data *td;
	struct afs_data_file *f;
	struct afs_filer_request *req;
	struct afs_job *job = t->job;

	/**
	 * check out the kernel (.so) first
	 */
	ret = get_replication_request(afs, t->koid, target_dev, &dev, &req);
	if (ret < 0)
		return ret;

	if (ret == 1) {
		req->task = t;
		ret = afs_filer_handle_replication(afs, req);

afs_task_log(t, "request data transfer(ino: %llu, from osd %d to %d) ret=%d\n",
		afs_llu(req->ino), dev, target_dev, ret);

		if (ret < 0) {
			free(req);
			return ret;
		}
		count++;
	}

	/**
	 * transfer input files.
	 */
	td = t->input;
	for (i = 0; i < td->n_files; i++) {
		req = NULL;
		f = td->files[i];
		ret = get_replication_request(afs, f->ino, target_dev, &dev, &req);
		if (ret < 0)
			return ret;
		else if (ret == 1) {
			req->task = t;
			ret = afs_filer_handle_replication(afs, req);

afs_task_log(t, "request data transfer(ino: %llu, from osd %d to %d) ret=%d\n",
		afs_llu(req->ino), dev, target_dev, ret);

			if (ret < 0) {
				free(req);
				return ret;
			}
			count++;
		}
	}

	/**
	 * for output files, we need to create objects in the target osd
	 * device. this can be done synchronously.
	 *
	 * updating metadata (as arbitrary placement by setting stloc) will be
	 * done by the callback function on the task completion.
	 */
	td = t->output;
	for (i = 0; i < td->n_files; i++) {
		int dev;
		struct afs_filer_request fr;
		struct afs_stripe sinfo;

		f = td->files[i];

		ret = afs_db_get_stripe_info(afs_db(afs), f->ino, &sinfo);
		if (ret)
			return -EINVAL;

		if (sinfo.stloc < 0)
			dev = f->ino % afs_osd(afs)->ndev;
		else
			dev = sinfo.stloc;

		/**
		 * if the output object has not been created on the target
		 * device, create one.
		 */
		if (dev != t->osd) {
			sinfo.stmode = AFS_STRIPE_NONE;
			sinfo.stloc = t->osd;

			memset((void *) &fr, 0, sizeof(fr));
			fr.ino = f->ino;
			fr.stripe = &sinfo;

			ret = afs_filer_handle_create(afs, &fr);
			if (ret) {
				/** the object maybe already exist */
				//return ret;
			}
		}
	}

	return count;
}

static inline void fsm_abandon_job(struct afs_task *t)
{
	struct afs_job *job = t->job;

	if (job->status == AFS_SCHED_TASK_ABANDONED)
		return;

afs_job_log(job, "aborting the job due to the task (%s at %p) failure..\n",
		t->name, t);

	job->status = AFS_SCHED_TASK_ABANDONED;
}

static inline void report_job_statistics(struct afs_job *job)
{
	uint64_t runtime, qtime;
	struct afs_task *task;

	runtime = job->t_complete - job->t_submit;

	afs_job_report(job, "\n===== JOB EXECUTION RESULT =====\n"
			"ID\t= %llu\nNAME\t= %s\n"
			"RUNTIME\t= %llu sec.\nSTATUS\t= %s\n",
			afs_llu(job->id), job->name,
			afs_llu(runtime), job->ret ? "aborted" : "success");

	afs_job_report(job, "\n===== RESULT OF EACH TASKS =====\n");

	list_for_each_entry(task, &job->task_list, list) {
		runtime = task->t_complete - task->t_submit;
		qtime = task->t_start - task->t_submit;
		afs_job_report(job, "[%s]\n"
			"RUNTIME\t= %llu sec (QTIME = %llu)\n"
			"AFE = %d (%llu, %llu)\n"
			"FILE TRANSFER = %llu\n"
			"TRANSFER SIZE = %llu bytes\n"
			"TRANSFER TIME = %llu sec.\n\n",
			task->name,
			afs_llu(runtime), afs_llu(qtime),
			task->osd,
			afs_llu(task->t_start), afs_llu(task->t_complete),
			afs_llu(task->n_transfers),
			afs_llu(task->bytes_transfers),
			afs_llu(task->t_transfer));
	}
}

/**
 * reclaim memory space for the job.
 * update lineage information.
 */
static int finish_job_execution(struct afs_ctx *afs, struct afs_job *job)
{
	int ret = 0;
	struct afs_task *task;

	if (job->ret == 0) {
		ret = afs_lineage_record_job_execution(afs, job);
afs_job_log(job, "job was successful, update lineage (ret = %d)\n", ret);
	}
	else {
afs_job_log(job, "job was not successful");
	}

	/**
	 * TODO: we need to report statistics here
	 */
	report_job_statistics(job);

	afs_parser_cleanup_job(job);

	return ret;
}

/**
 * handles:
 * successful cases: AFS_SCHED_TASK_COMPLETE, AFS_SCHED_TASK_SKIP
 * failure cases: AFS_SCHED_TASK_ABORT, AFS_SCHED_TASK_ABANDONED
 *
 * update file metadata for output files accordingly. (done by the callback)
 * update lineage db.
 * check whether whole job has been processed. 
 */
static int fsm_handle_task_completion(struct afs_ctx *afs, struct afs_task *t)
{
	int ret = 0;
	int in_progress = 0;
	struct afs_job *job = t->job;
	struct afs_task *task;

	if (t->status == AFS_SCHED_TASK_ABORT)
		fsm_abandon_job(t);

	fsm_qtask(t, Q_COMPLETE);

	/**
	 * check whether all tasks are in the complete queue.
	 */
	list_for_each_entry(task, &job->task_list, list) {
		if (task->q != Q_COMPLETE) {
			in_progress = 1;
			break;
		}
	}

	/**
	 * Is whole job finished?
	 */
	if (in_progress == 0) {
		jq_lock(Q_RUNNING);
		list_del(&job->list);
		jq_unlock(Q_RUNNING);

		job->t_complete = afs_now();

		/**
		 * the only failure might happen is that updating lineage db
		 */
		ret = finish_job_execution(afs, job);
	}

out:
	return 0;
}

/**
 * advancer, finite state machine implementation.
 */
static void *sched_worker_advancer(void *arg)
{
	struct afs_ctx *afs = (struct afs_ctx *) arg;
	int ret;
	int ndev = afs_osd(afs)->ndev;
	struct afs_task *t;
	struct afs_osd_request *r;

	while (1) {
		t = fsm_fetch(Q_WAITING);
		if (!t) {
			usleep(1000);
			continue;
		}

		while (1) {
			int exit = 0;

			pthread_mutex_lock(&t->stlock);

			/**
			 * first, check the job has been abandoned. the task
			 * should be abandoned as well.
			 */
			if (t->job->status == AFS_SCHED_TASK_ABANDONED)
				t->status = AFS_SCHED_TASK_ABANDONED;

			switch (t->status) {
			case AFS_SCHED_TASK_INIT:
			case AFS_SCHED_TASK_BLOCKED:
				if (fsm_task_input_produced(t)) {
					t->status = AFS_SCHED_TASK_AVAIL;
afs_task_log(t, "all input files are available.\n");
				}
				else {
					fsm_qtask(t, Q_WAITING);
					exit = 1;
				}
				break;

			case AFS_SCHED_TASK_AVAIL:
				fsm_schedule_input_lazy(afs, t);
				ret = fsm_request_data_transfer(afs, t);
				if (ret > 0) {
					//pthread_mutex_lock(&t->stlock);
					t->io_inflight += ret;
					t->status = AFS_SCHED_TASK_WAITIO;
					//pthread_mutex_unlock(&t->stlock);
afs_task_log(t, "%d data transfers are requested.\n", ret);
				}
				else if (ret < 0) {
					fsm_qtask(t, Q_WAITING);
					exit = 1;
afs_task_log(t, "data transfers request failed(%d).\n", ret);
				}
				else {
					t->status = AFS_SCHED_TASK_READY;
afs_task_log(t, "no more transfer needed, task is ready to submit.\n");
				}
				break;

			case AFS_SCHED_TASK_WAITIO:
				fsm_qtask(t, Q_WAITING);
				exit = 1;
				break;

			case AFS_SCHED_TASK_READY:
				/**
				 * putting into q should come first,
				 * considering super-fast or failing tasks:
				 * their callback would try to remove the task
				 * from the running q.
				 */
				fsm_qtask(t, Q_RUNNING);
				ret = fsm_request_task_execution(afs, t);
				if (ret) {
					tq_lock(Q_RUNNING);
					list_del(&t->tqlink);
					tq_unlock(Q_RUNNING);
				}
afs_task_log(t, "task submitted to osd %d (ret = %d)\n", t->osd, ret);
				exit = 1;
				break;

			case AFS_SCHED_TASK_SKIP:
			case AFS_SCHED_TASK_COMPLETE:
			case AFS_SCHED_TASK_ABORT:
			case AFS_SCHED_TASK_ABANDONED:
				ret = fsm_handle_task_completion(afs, t);
				exit = 1;
				break;

#if 0
			case AFS_SCHED_TASK_SKIP:
			case AFS_SCHED_TASK_COMPLETE:
				ret = fsm_handle_task_completion(afs, t);
				fsm_qtask(t, ret ? Q_WAITING : Q_COMPLETE);
				exit = 1;
				break;

			case AFS_SCHED_TASK_ABORT:
				fsm_handle_task_error(afs, t);
				fsm_qtask(t, Q_COMPLETE);
				exit = 1;
				break;
#endif

			/** this should not be in a wait queue */
			case AFS_SCHED_TASK_RUNNING:
			default:
				exit = 1;
				break;
			}

			pthread_mutex_unlock(&t->stlock);

			if (exit)
				break;
		}
	}

	return (void *) 0;
}

/**
 * afs_sched_rr is a naive job assigning policy, which assigns blindly in a
 * round-robin fashion. if valid affinity is specified, it will be applied
 * here.
 *
 * @afs: afs_ctx.
 * @job: afs_job to be scheduled.
 *
 * always returns 0, obviously no reasons to fail.
 */
static int afs_sched_rr(struct afs_ctx *afs, struct afs_job *job)
{
	int i = 0, ndev;
	struct afs_task *t;

	ndev = afs_osd(afs)->ndev;

	list_for_each_entry(t, &job->task_list, list) {
		t->status = fsm_task_input_produced(t) ?
				AFS_SCHED_TASK_AVAIL : AFS_SCHED_TASK_BLOCKED;

		if (t->affinity >= 0 && t->affinity < ndev)
			t->osd = t->affinity;
		else
			t->osd = i++;

afs_task_log(t, "scheduled to osd %d\n", t->osd);

		if (i == ndev)
			i = 0;
	}

	return 0;
}

static int afs_sched_input(struct afs_ctx *afs, struct afs_job *job)
{
	struct afs_task *t;

	list_for_each_entry(t, &job->task_list, list) {
		t->status = fsm_task_input_produced(t) ?
				AFS_SCHED_TASK_AVAIL : AFS_SCHED_TASK_BLOCKED;

		if (t->affinity >= 0 && t->affinity < afs_osd(afs)->ndev) {
			t->osd = t->affinity;
afs_task_log(t, "scheduled to osd %d (user affinity).\n", t->osd);
		}
		else {
			t->osd = AFS_SCHED_BIND_LAZY;
afs_task_log(t, "lazy scheduled, will assign osd later.\n");
		}
	}

	return 0;
}

/**
 * external interface implementation.
 */

static void cancel_threads(struct afs_sched *self)
{
	void *res;

	(void) pthread_cancel(self->w_advancer);
	(void) pthread_cancel(self->w_preparer);

	(void) pthread_join(self->w_advancer, &res);
	(void) pthread_join(self->w_preparer, &res);
}

int afs_sched_init(struct afs_sched *self)
{
	int i;
	int ret;
	struct afs_ctx *afs;

	for (i = 0; i < Q_LEN; i++) {
		pthread_mutex_init(&jql[i], NULL);
		pthread_mutex_init(&tql[i], NULL);
		INIT_LIST_HEAD(&jq[i]);
		INIT_LIST_HEAD(&tq[i]);
	}

	/** spawn workers */
	ret = pthread_create(&self->w_preparer, NULL, &sched_worker_preparer,
				afs_ctx(self, sched));
	if (ret)
		goto cancel_out;
	ret = pthread_create(&self->w_advancer, NULL, &sched_worker_advancer,
				afs_ctx(self, sched));
	if (ret)
		goto cancel_out;

	afs = afs_ctx(self, sched);

	self->policy = afs_config(afs)->sched_policy;
	if (self->policy >= AFS_SCHED_N_POLICIES) {
		self->policy = AFS_SCHED_POLICY_RR;

		/** warn this */
	}

	pthread_mutex_init(&g_jobid_lock, NULL);

	return 0;

cancel_out:
	cancel_threads(self);
	for (i = 0; i < Q_LEN; i++) {
		pthread_mutex_destroy(&tql[i]);
		pthread_mutex_destroy(&jql[i]);
	}
	return errno;
}

void afs_sched_exit(struct afs_sched *self)
{
	int i;
	/** XXX: what shall we do on pending/processing jobs? */

	pthread_mutex_destroy(&g_jobid_lock);

	cancel_threads(self);
	for (i = 0; i < Q_LEN; i++) {
		pthread_mutex_destroy(&tql[i]);
		pthread_mutex_destroy(&jql[i]);
	}
}

int afs_sched_submit_job(struct afs_sched *self, const uint64_t ino)
{
	int ret = 0;
	struct afs_ctx *ctx = afs_ctx(self, sched);
	struct afs_stripe sinfo;
	struct afs_filer_request req;
	struct afs_job *job = NULL;
	char *buf;

	/** XXX: we read the whole contents into our buffer.
	 * How big can this job description script??
	 */
	ret = afs_db_get_stripe_info(afs_db(ctx), ino, &sinfo);
	buf = afs_malloc(sinfo.stat.st_size);

	memset(&req, 0, sizeof(req));

	req.stripe = &sinfo;
	req.ino = ino;
	req.buf = buf;
	req.size = sinfo.stat.st_size;
	req.off = 0;

	ret = afs_filer_handle_read(ctx, &req);
	if (ret)
		goto out_err;

	ret = afs_parser_parse_script(buf, req.size, &job);
	if (ret)
		goto out_err;

	job->id = next_job_id();

	/** open the log stream */
	if (afs_job_log_open(job))
		goto out_err;

	/** validate whether all data files are ready */
	ret = validate_data_files(ctx, job);
	if (ret) {
afs_job_log(job, "data file validation failed: %d\n", ret);
		goto out_err;
	}

	afs_job_log_dump(job);
afs_job_log(job, "job successfully submitted (sched: %d)\n", job->sched);

	jq_lock(Q_WAITING);
	jq_append(job, Q_WAITING);
	jq_unlock(Q_WAITING);

	return 0;

out_err:
	if (job)
		afs_parser_cleanup_job(job);
	return ret;
}

