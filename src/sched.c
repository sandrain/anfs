/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * the main scheduler of activefs.
 */
#include "anfs.h"

enum {
	Q_WAITING	= 0,
	Q_RUNNING,
	Q_COMPLETE,
	Q_LEN,
};

typedef	int (*anfs_sched_func_t) (struct anfs_ctx *, struct anfs_job *);

/**
 * pathdb handling helpers
 */

static inline int pathdb_insert(struct anfs_ctx *ctx, uint64_t ino)
{
	char path[2048];	/* should be enough for now */
	int ret = anfs_mdb_get_full_path(anfs_mdb(ctx), ino, path);

	return anfs_pathdb_insert(anfs_pathdb(ctx),
			anfs_config(ctx)->partition,
			ino + ANFS_OBJECT_OFFSET, path);
}

static inline int pathdb_update(struct anfs_ctx *ctx, uint64_t ino)
{
	char path[2048];
	int ret = anfs_mdb_get_full_path(anfs_mdb(ctx), ino, path);

	return anfs_pathdb_update(anfs_pathdb(ctx),
			anfs_config(ctx)->partition,
			ino + ANFS_OBJECT_OFFSET, path);
}

static inline int pathdb_remove(struct anfs_ctx *ctx, uint64_t ino)
{
	return anfs_pathdb_remove(anfs_pathdb(ctx),
			anfs_config(ctx)->partition,
			ino + ANFS_OBJECT_OFFSET);
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

static inline void jq_append(struct anfs_job *job, int q)
{
	switch (q) {
	case Q_WAITING: job->t_submit = anfs_now(); break;
	case Q_RUNNING: job->t_start = anfs_now(); break;
	case Q_COMPLETE: job->t_complete = anfs_now(); break;
	default: return;
	}

	list_add_tail(&job->list, &jq[q]);
}

static inline struct anfs_job *jq_fetch(int q)
{
	struct anfs_job *job;

	if (list_empty(&jq[q]))
		return NULL;

	job = list_first_entry(&jq[q], struct anfs_job, list);
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

static inline void tq_append(struct anfs_task *task, int q)
{
#if 0
	switch (q) {
	case Q_WAITING: task->t_submit = anfs_now(); break;
	case Q_RUNNING: task->t_start = anfs_now(); break;
	case Q_COMPLETE: task->t_complete = anfs_now(); break;
	default: return;
	}
#endif

	task->q = q;
	list_add_tail(&task->tqlink, &tq[q]);
}

static inline struct anfs_task *tq_fetch(int q)
{
	struct anfs_task *task;

	if (list_empty(&tq[q]))
		return NULL;

	task = list_first_entry(&tq[q], struct anfs_task, tqlink);
	list_del(&task->tqlink);

	return task;
}

static int anfs_sched_rr(struct anfs_ctx *afs, struct anfs_job *job);
static int anfs_sched_input(struct anfs_ctx *afs, struct anfs_job *job);

/**
 * find_inode returns inode # of the given @path, which should be rooted by the
 * mounting point.
 *
 * @afs: anfs_ctx.
 * @path: path rooted by the mounting point.
 *
 * returns inode # if found, 0 otherwise.
 */
static uint64_t find_inode(struct anfs_ctx *afs, const char *path,
			struct anfs_data_file *file)
{
	int ret;
	uint64_t ino;
	struct stat stbuf;

	ret = anfs_mdb_getattr(anfs_mdb(afs), path, &stbuf);
	if (!file)
		return ret ? 0 : stbuf.st_ino;
	ino = stbuf.st_ino;

	file->ino = ino;
	file->osd = stbuf.st_blksize == 4096 ?
			ino % anfs_osd(afs)->ndev : (int) stbuf.st_blksize;
	file->size = stbuf.st_size;

	return ino;
}

static int validate_data_files(struct anfs_ctx *afs, struct anfs_job *job)
{
	int i, count;
	uint64_t ino;
	struct anfs_task *t;
	struct anfs_task_data *td;
	struct anfs_data_file *df;

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

static anfs_sched_func_t sched_funcs[] = 
{
	&anfs_sched_rr,
	&anfs_sched_input,
	&anfs_sched_input,	/** minwait */
};

static inline int schedule(struct anfs_ctx *afs, struct anfs_job *job)
{
#if 0
	int policy = anfs_sched(afs)->policy;
#endif
	int policy = job->sched;

	anfs_sched_func_t func = sched_funcs[policy];
	return (*func) (afs, job);
}

/**
 * osd_task_complete_callback just change the task status and put rest of the
 * things to the advancer.
 *
 * @status: task return code (0 on success).
 * @arg: the task descriptor.
 */
static void osd_task_complete_callback(int status, struct anfs_osd_request *r)
{
	uint32_t i;
	struct anfs_task *t = r->task;

	tq_lock(Q_RUNNING);
	list_del(&t->tqlink);
	tq_unlock(Q_RUNNING);

	t->ret = status;

anfs_task_log(t, "task execution complete from osd %d (tid = %llu, ret = %d)\n",
		t->osd, anfs_llu(t->tid), status);

	if (status)
		t->status = ANFS_SCHED_TASK_ABORT;
	else {
		t->status = ANFS_SCHED_TASK_COMPLETE;

		/** 
		 * handling output files:
		 * . mark that they are available
		 * . adjust the size by checking with osd
		 * . set the location of the file in inode
		 */
		for (i = 0; i < t->output->n_files; i++) {
			int ret = 0;
			uint64_t size;
			struct anfs_ctx *afs = (struct anfs_ctx *) r->priv;
			struct anfs_data_file *file = t->output->files[i];

			ret = anfs_osd_get_object_size(anfs_osd(afs), r->dev,
						r->partition, file->ino,
						&size);
			if (ret) {
				/** 
				 * XXX: is this correct?
				 */
				t->status = ANFS_SCHED_TASK_ABORT;
				goto out;
			}

			ret = anfs_mdb_update_task_output_file(anfs_mdb(afs),
						file->ino, t->osd, size);
			file->osd = t->osd;	/** update current position */
			file->size = size;

anfs_task_log(t, "update output file metadata (ino=%llu, dev=%d, size=%llu)"
		", (ret = %d)\n",
		 anfs_llu(file->ino), t->osd, anfs_llu(size), ret);

			if (ret) {
				t->status = ANFS_SCHED_TASK_ABORT;
				goto out;
			}

			ret = anfs_mdb_invalidate_replica(anfs_mdb(afs),
							file->ino);

anfs_task_log(t, "invalidate replicas for output file (ino=%llu)"
		", (ret = %d)\n", anfs_llu(file->ino), ret);

			if (ret) {
				t->status = ANFS_SCHED_TASK_ABORT;
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
static int prepare_collections(struct anfs_ctx *afs, struct anfs_task *t)
{
	int ret;
	uint64_t cids[2];
	struct anfs_osd *anfs_osd = anfs_osd(afs);
	struct anfs_job *job = t->job;

	ret = anfs_mdb_assign_collection_ids(anfs_mdb(afs), 2, cids);
	if (ret)
		return ret;

	ret = anfs_osd_create_collection(anfs_osd, t->osd, 0x22222, &cids[0]);
	ret |= anfs_osd_create_collection(anfs_osd, t->osd, 0x22222, &cids[1]);

anfs_task_log(t, "input/output collections create (ret=%d).\n", ret);

	if (!ret) {
		t->input_cid = cids[0];
		t->output_cid = cids[1];
	}

	return ret;
}

static int fsm_request_task_execution(struct anfs_ctx *afs, struct anfs_task *t)
{
	int ret;
	struct anfs_osd_request *r;
	uint32_t i;
	uint64_t partition = anfs_config(afs)->partition;
	uint64_t *inobjs, *outobjs;
	uint32_t n_ins = t->input->n_files;
	uint32_t n_outs = t->output->n_files;
	struct anfs_data_file *file;

	/** create collections */
	ret = prepare_collections(afs, t);
	if (ret)
		return ret;

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

	ret = anfs_osd_set_membership(anfs_osd(afs), t->osd, partition,
				t->input_cid, inobjs, n_ins);
anfs_task_log(t, "membership set for input %d objects (ret=%d).\n",
		t->input->n_files, ret);
	if (ret)
		goto out_free_olist;
	ret = anfs_osd_set_membership(anfs_osd(afs), t->osd, partition,
				t->output_cid, outobjs, n_outs);
anfs_task_log(t, "membership set for output %d objects (ret=%d).\n",
		t->output->n_files, ret);
	if (ret)
		goto out_free_olist;

	/** prepare and submit request */
	r = calloc(1, sizeof(*r));
	if (!r) {
		ret = -ENOMEM;
		goto out_free_olist;
	}

	r->type = ANFS_OSD_RQ_EXECUTE;
	r->dev = t->osd;
	r->task = t;
	r->priv = afs;
	r->callback = &osd_task_complete_callback;

	return anfs_osd_submit_request(anfs_osd(afs), r);

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
	struct anfs_job *job;
	struct anfs_task *t;
	struct anfs_ctx *afs = (struct anfs_ctx *) arg;

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
			ret = anfs_lineage_scan_reuse(afs, t);
			t->status = ret ?
				ANFS_SCHED_TASK_SKIP : ANFS_SCHED_TASK_INIT;

			//ret = prepare_collections(afs, t);

			pthread_mutex_init(&t->stlock, NULL);

			tq_lock(Q_WAITING);
			tq_append(t, Q_WAITING);
			tq_unlock(Q_WAITING);
		}

		jq_lock(Q_RUNNING);
		jq_append(job, Q_RUNNING);
		jq_unlock(Q_RUNNING);

#if 0
		ret = anfs_virtio_create_job_entry(anfs_virtio(afs), job);
		if (!ret) {
		}
#endif
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

static inline struct anfs_task *fsm_fetch(int q)
{
	struct anfs_task *t;

	tq_lock(q);
	t = tq_fetch(q);
	tq_unlock(q);

	return t;
}

static inline void fsm_qtask(struct anfs_task *t, int q)
{
	tq_lock(q);
	tq_append(t, q);
	tq_unlock(q);
}

static inline int fsm_task_input_produced(struct anfs_task *t)
{
	int i;
	struct anfs_task_data *input = t->input;

	/** check necessary files are all created. */
	for (i = 0; i < input->n_files; i++)
		if (!input->files[i]->available)
			return 0;
#if 0
	/** this pollutes log too much */
		else {
anfs_task_log(t, "input %s is ready in osd %d.\n", input->files[i]->path,
			input->files[i]->osd);
		}
#endif

	/**
	 * if no input files specified, return as if they were produced
	 * already.
	 */

	return 1;
}

static void fsm_replication_callback(int status, struct anfs_copy_request *req)
{
	int ret;
	struct anfs_ctx *ctx = (struct anfs_ctx *) req->priv;
	struct anfs_task *t = req->task;
	struct anfs_job *job = t->job;

	if (status == 0) {
		ret = anfs_mdb_add_replication(anfs_mdb(ctx), req->ino, req->dest);
		if (ret) {
			/** XXX: what a mess! what should we do? */
anfs_task_log(t, "file replication (%llu) seems to be failed (ret=%d) !!!!",
		anfs_llu(req->ino), status);
		}

		t->t_transfer += req->t_complete - req->t_submit;
		t->n_transfers += 1;
	}

	/** this field is continuously accessed/checked by the advancer */
	pthread_mutex_lock(&t->stlock);
	t->io_inflight--;
	if (t->io_inflight <= 0)
		req->task->status = ANFS_SCHED_TASK_READY;
	pthread_mutex_unlock(&t->stlock);

anfs_task_log(t, "data transfer finished for ino %llu (status=%d), "
		 "%d in-flight io presents\n",
		anfs_llu(req->ino), status, t->io_inflight);

	free(req);
}

/**
 * returns:
 * 0 if no replication is required
 * 1 if replication is necessary, and the request structure has been
 * sucessfully initialized.
 * negatives on errors.
 */
static int get_replication_request(struct anfs_ctx *afs, uint64_t oid, int tdev,
				int *dev_out, struct anfs_copy_request **out_req)
{
	int ret = 0;
	int dev;
	uint64_t ino = anfs_o2i(oid);
	struct anfs_mdb *db = anfs_mdb(afs);
	struct anfs_copy_request *req = NULL;

	ret = anfs_mdb_get_file_location(anfs_mdb(afs), ino, &dev);
	if (ret)
		return -EINVAL;

	if (dev == tdev)
		return 0;		/** replication avail */

	ret = anfs_mdb_replication_available(db, oid, tdev);
	if (ret < 0)
		return -EIO;
	else if (ret > 0)
		return 0;		/** replication avail */

	req = calloc(1, sizeof(*req));
	if (!req)
		return -ENOMEM;

	req->ino = ino;
	req->src = dev;
	req->dest = tdev;
	req->callback = &fsm_replication_callback;
	req->priv = afs;
	/** req->task should be set by caller */

	if (dev_out)
		*dev_out = dev;
	*out_req = req;
	return 1;
}

static int sched_input_assign_osd(struct anfs_ctx *afs, struct anfs_task *t)
{
	uint32_t i;
	int max = 0;
	uint64_t *datapos, maxval = 0;
	struct anfs_task_data *input;
	struct anfs_data_file *file;

	datapos = anfs_calloc(anfs_osd(afs)->ndev, sizeof(uint64_t));

	input = t->input;
	for (i = 0; i < input->n_files; i++) {
		file = input->files[i];
		datapos[file->osd] += file->size;
	}

	for (i = 0; i < anfs_osd(afs)->ndev; i++) {
anfs_task_log(t, " -- osd[%d] = %llu bytes\n", i, anfs_llu(datapos[i]));
		if (datapos[i] > maxval) {
			maxval = datapos[i];
			max = i;
		}
	}
anfs_task_log(t, " -- ## task is scheduled to osd %d\n", max);

	free(datapos);

anfs_task_log(t, "task is scheduled to osd %d\n", max);
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

static double calculate_transfer_cost(struct anfs_ctx *afs, struct anfs_task *t,
					int osd)
{
	uint32_t i;
	uint64_t bytes = 0, wait;
	struct anfs_task_data *input = t->input;
	struct anfs_data_file *file;

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
static const char *devq_dir = "/ccs/techint/home/hs2/anfs_eval/devq/";
static const char *osd_hostnames[] = {
	"atom-a1", "atom-a2", "atom-b1", "atom-b2",
	"atom-c1", "atom-c2", "atom-d1", "atom-d2"
};

static FILE *dfps[8];

static double calculate_queue_cost(struct anfs_ctx *afs, int osd)
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

static inline void minwait_update_time(struct anfs_ctx *afs)
{
	uint32_t i;
	uint64_t old = minwait_timestamp;
	struct timeval tv;

	gettimeofday(&tv, NULL);
	minwait_timestamp = tv.tv_sec + tv.tv_usec * 0.000001;

	for (i = 0; i < anfs_osd(afs)->ndev; i++) {
		wait_time[i] -= minwait_timestamp - old;
		if (wait_time[i] < 0)
			wait_time[i] = 0;
	}
}

static inline double minwait_get_wait_time(struct anfs_ctx *afs, int osd)
{
	minwait_update_time(afs);
	return wait_time[osd];
}

static inline double get_task_runtime(struct anfs_task *t)
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

static int sched_minwait_assign_osd(struct anfs_ctx *afs, struct anfs_task *t)
{
	uint32_t i;
	int min = 0;
	double minval = (double) UINT64_MAX;
	double dt[MINWAIT_MAXOSD];
	double wait[MINWAIT_MAXOSD];

	memset(wait, 0x00, sizeof(uint64_t)*MINWAIT_MAXOSD);

	for (i = 0; i < anfs_osd(afs)->ndev; i++) {
		dt[i] = calculate_transfer_cost(afs, t, i);
		wait[i] = dt[i];
		wait[i] += minwait_get_wait_time(afs, i);

anfs_task_log(t, " -- osd[%d] = %lf seconds wait\n", i, wait[i]);

		if (wait[i] <= minval) {	/** whenever there is a tie, change */
			min = i;
			minval = wait[i];
		}
	}

	wait_time[min] += dt[min] + get_task_runtime(t);
	t->mw_submit = minwait_timestamp;

anfs_task_log(t, " -- ## task submitted to osd %d\n", min);

	return min;
}

/**
 * FIXME: each scheduling policy has to implement function table.
 */
static inline void fsm_schedule_input_lazy(struct anfs_ctx *afs,
						struct anfs_task *t)
{
	int policy = t->job->sched;

	if (policy == ANFS_SCHED_POLICY_RR)
		return;

	switch (policy) {
		case ANFS_SCHED_POLICY_INPUT:
			t->osd = sched_input_assign_osd(afs, t);
			break;
		case ANFS_SCHED_POLICY_MINWAIT:
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
static int fsm_request_data_transfer(struct anfs_ctx *afs, struct anfs_task *t)
{
	int ret = 0;
	int i, count = 0;
	int dev, target_dev = t->osd;
	struct anfs_task_data *td;
	struct anfs_data_file *f;
	struct anfs_copy_request *req;
	struct anfs_job *job = t->job;

	/**
	 * check out the kernel (.so) first
	 */
	ret = get_replication_request(afs, t->koid, target_dev, &dev, &req);
	if (ret < 0)
		return ret;

	if (ret == 1) {
		req->task = t;
		anfs_store_request_copy(anfs_store(afs), req);
		count++;

anfs_task_log(t, "request data transfer(ino: %llu, from osd %d to %d) ret=%d\n",
		anfs_llu(req->ino), dev, target_dev, ret);
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
			anfs_store_request_copy(anfs_store(afs), req);
			count++;

anfs_task_log(t, "request data transfer(ino: %llu, from osd %d to %d) ret=%d\n",
		anfs_llu(req->ino), dev, target_dev, ret);
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
		int dev, index;

		ret = anfs_mdb_get_file_location(anfs_mdb(afs), f->ino,
						&index);
		if (ret)
			return -EINVAL;

		dev = index == -1 ? f->ino % anfs_osd(afs)->ndev : index;


		/**
		 * if the output object has not been created on the target
		 * device, create one.
		 */
		if (dev != t->osd) {
			ret = anfs_store_create(anfs_store(afs), f->ino, t->osd);
			if (ret) {
				/** the object maybe already exist */
				//return ret;
			}
		}
	}

	return count;
}

static inline void fsm_abandon_job(struct anfs_task *t)
{
	struct anfs_job *job = t->job;

	if (job->status == ANFS_SCHED_TASK_ABANDONED)
		return;

anfs_job_log(job, "aborting the job due to the task (%s at %p) failure..\n",
		t->name, t);

	job->status = ANFS_SCHED_TASK_ABANDONED;
}

static inline void report_job_statistics(struct anfs_job *job)
{
	uint64_t runtime, qtime;
	struct anfs_task *task;

	runtime = job->t_complete - job->t_submit;

	anfs_job_report(job, "\n===== JOB EXECUTION RESULT =====\n"
			"ID\t= %llu\nNAME\t= %s\n"
			"RUNTIME\t= %llu sec.\nSTATUS\t= %s\n",
			anfs_llu(job->id), job->name,
			anfs_llu(runtime), job->ret ? "aborted" : "success");

	anfs_job_report(job, "\n===== RESULT OF EACH TASKS =====\n");

	list_for_each_entry(task, &job->task_list, list) {
		runtime = task->t_complete - task->t_submit;
		qtime = task->t_start - task->t_submit;
		anfs_job_report(job, "[%s]\n"
			"RUNTIME\t= %llu sec (QTIME = %llu)\n"
			"AFE = %d (%llu, %llu)\n"
			"FILE TRANSFER = %llu\n"
			"TRANSFER SIZE = %llu bytes\n"
			"TRANSFER TIME = %llu sec.\n\n",
			task->name,
			anfs_llu(runtime), anfs_llu(qtime),
			task->osd,
			anfs_llu(task->t_start), anfs_llu(task->t_complete),
			anfs_llu(task->n_transfers),
			anfs_llu(task->bytes_transfers),
			anfs_llu(task->t_transfer));
	}
}

/**
 * reclaim memory space for the job.
 * update lineage information.
 */
static int finish_job_execution(struct anfs_ctx *afs, struct anfs_job *job)
{
	int ret = 0;
	struct anfs_task *task;

	if (job->ret == 0) {
		ret = anfs_lineage_record_job_execution(afs, job);
anfs_job_log(job, "job was successful, update lineage (ret = %d)\n", ret);
	}
	else {
anfs_job_log(job, "job was not successful");
	}

	/**
	 * TODO: we need to report statistics here
	 */
	report_job_statistics(job);

	anfs_parser_cleanup_job(job);

	return ret;
}

/**
 * handles:
 * successful cases: ANFS_SCHED_TASK_COMPLETE, ANFS_SCHED_TASK_SKIP
 * failure cases: ANFS_SCHED_TASK_ABORT, ANFS_SCHED_TASK_ABANDONED
 *
 * update file metadata for output files accordingly. (done by the callback)
 * update lineage db.
 * check whether whole job has been processed. 
 */
static int fsm_handle_task_completion(struct anfs_ctx *afs, struct anfs_task *t)
{
	int ret = 0;
	int in_progress = 0;
	struct anfs_job *job = t->job;
	struct anfs_task *task;

	if (t->status == ANFS_SCHED_TASK_ABORT)
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

		job->t_complete = anfs_now();

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
	struct anfs_ctx *afs = (struct anfs_ctx *) arg;
	int ret;
	int ndev = anfs_osd(afs)->ndev;
	struct anfs_task *t;
	struct anfs_osd_request *r;

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
			if (t->job->status == ANFS_SCHED_TASK_ABANDONED)
				t->status = ANFS_SCHED_TASK_ABANDONED;

			switch (t->status) {
			case ANFS_SCHED_TASK_INIT:
			case ANFS_SCHED_TASK_BLOCKED:
				if (fsm_task_input_produced(t)) {
					t->status = ANFS_SCHED_TASK_AVAIL;
anfs_task_log(t, "all input files are available.\n");
				}
				else {
					fsm_qtask(t, Q_WAITING);
					exit = 1;
				}
				break;

			case ANFS_SCHED_TASK_AVAIL:
				fsm_schedule_input_lazy(afs, t);
				ret = fsm_request_data_transfer(afs, t);
				if (ret > 0) {
					t->io_inflight += ret;
					t->status = ANFS_SCHED_TASK_WAITIO;
anfs_task_log(t, "%d data transfers are requested.\n", ret);
				}
				else if (ret < 0) {
					fsm_qtask(t, Q_WAITING);
					exit = 1;
anfs_task_log(t, "data transfers request failed(%d).\n", ret);
				}
				else {
					t->status = ANFS_SCHED_TASK_READY;
anfs_task_log(t, "no more transfer needed, task is ready to submit.\n");
				}
				break;

			case ANFS_SCHED_TASK_WAITIO:
				fsm_qtask(t, Q_WAITING);
				exit = 1;
				break;

			case ANFS_SCHED_TASK_READY:
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
anfs_task_log(t, "task submitted to osd %d (ret = %d)\n", t->osd, ret);
				exit = 1;
				break;

			case ANFS_SCHED_TASK_SKIP:
			case ANFS_SCHED_TASK_COMPLETE:
			case ANFS_SCHED_TASK_ABORT:
			case ANFS_SCHED_TASK_ABANDONED:
				ret = fsm_handle_task_completion(afs, t);
				exit = 1;
				break;

			/** this should not be in a wait queue */
			case ANFS_SCHED_TASK_RUNNING:
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
 * anfs_sched_rr is a naive job assigning policy, which assigns blindly in a
 * round-robin fashion. if valid affinity is specified, it will be applied
 * here.
 *
 * @afs: anfs_ctx.
 * @job: anfs_job to be scheduled.
 *
 * always returns 0, obviously no reasons to fail.
 */
static int anfs_sched_rr(struct anfs_ctx *afs, struct anfs_job *job)
{
	int i = 0, ndev;
	struct anfs_task *t;

	ndev = anfs_osd(afs)->ndev;

	list_for_each_entry(t, &job->task_list, list) {
		t->status = fsm_task_input_produced(t) ?
				ANFS_SCHED_TASK_AVAIL : ANFS_SCHED_TASK_BLOCKED;

		if (t->affinity >= 0 && t->affinity < ndev)
			t->osd = t->affinity;
		else
			t->osd = i++;

anfs_task_log(t, "scheduled to osd %d\n", t->osd);

		if (i == ndev)
			i = 0;
	}

	return 0;
}

static int anfs_sched_input(struct anfs_ctx *afs, struct anfs_job *job)
{
	struct anfs_task *t;

	list_for_each_entry(t, &job->task_list, list) {
		t->status = fsm_task_input_produced(t) ?
				ANFS_SCHED_TASK_AVAIL : ANFS_SCHED_TASK_BLOCKED;

		if (t->affinity >= 0 && t->affinity < anfs_osd(afs)->ndev) {
			t->osd = t->affinity;
anfs_task_log(t, "scheduled to osd %d (user affinity).\n", t->osd);
		}
		else {
			t->osd = ANFS_SCHED_BIND_LAZY;
anfs_task_log(t, "lazy scheduled, will assign osd later.\n");
		}
	}

	return 0;
}

/**
 * external interface implementation.
 */

static void cancel_threads(struct anfs_sched *self)
{
	void *res;

	(void) pthread_cancel(self->w_advancer);
	(void) pthread_cancel(self->w_preparer);

	(void) pthread_join(self->w_advancer, &res);
	(void) pthread_join(self->w_preparer, &res);
}

int anfs_sched_init(struct anfs_sched *self)
{
	int i;
	int ret;
	struct anfs_ctx *afs;

	for (i = 0; i < Q_LEN; i++) {
		pthread_mutex_init(&jql[i], NULL);
		pthread_mutex_init(&tql[i], NULL);
		INIT_LIST_HEAD(&jq[i]);
		INIT_LIST_HEAD(&tq[i]);
	}

	/** spawn workers */
	ret = pthread_create(&self->w_preparer, NULL, &sched_worker_preparer,
				anfs_ctx(self, sched));
	if (ret)
		goto cancel_out;
	ret = pthread_create(&self->w_advancer, NULL, &sched_worker_advancer,
				anfs_ctx(self, sched));
	if (ret)
		goto cancel_out;

	afs = anfs_ctx(self, sched);

	self->policy = anfs_config(afs)->sched_policy;
	if (self->policy >= ANFS_SCHED_N_POLICIES) {
		self->policy = ANFS_SCHED_POLICY_RR;

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

void anfs_sched_exit(struct anfs_sched *self)
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

int anfs_sched_submit_job(struct anfs_sched *self, const uint64_t ino)
{
	int ret = 0;
	struct anfs_ctx *ctx = anfs_ctx(self, sched);
	FILE *fp;
	struct anfs_job *job = NULL;
	struct stat stbuf;
	char pathbuf[PATH_MAX];
	char *buf;

	anfs_store_get_path(anfs_store(ctx), ino, -1, pathbuf);
	stat(pathbuf, &stbuf);
	buf = anfs_malloc(stbuf.st_size + 1);

	if ((fp = fopen(pathbuf, "r")) == NULL)
		goto out_err;

	fread(buf, stbuf.st_size, 1, fp);
	fclose(fp);
	buf[stbuf.st_size] = 0;

	ret = anfs_parser_parse_script(buf, stbuf.st_size, &job);
	if (ret)
		goto out_err;

	job->id = next_job_id();

	/** open the log stream */
	if (anfs_job_log_open(job))
		goto out_err;

	/** validate whether all data files are ready */
	ret = validate_data_files(ctx, job);
	if (ret) {
anfs_job_log(job, "data file validation failed: %d\n", ret);
		goto out_err;
	}

	anfs_job_log_dump(job);
anfs_job_log(job, "job successfully submitted (sched: %d)\n", job->sched);

	jq_lock(Q_WAITING);
	jq_append(job, Q_WAITING);
	jq_unlock(Q_WAITING);

	return 0;

out_err:
	if (job)
		anfs_parser_cleanup_job(job);
	return ret;
}

