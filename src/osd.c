/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * NOTE that all functions should be thread-safe.
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <arpa/inet.h>
#include <endian.h>
#include <pthread.h>
//#include <open-osd/libosd.h>

#include "anfs.h"

static void osdblk_make_credential(u8 *creds, struct osd_obj_id *obj,
				   bool is_v1)
{
	osd_sec_init_nosec_doall_caps(creds, obj, false, is_v1);
}

static int osdblk_exec(struct osd_request *or, u8 *cred)
{
	struct osd_sense_info osi;
	int ret;

	ret = osd_finalize_request(or, 0, cred, NULL);
	if (ret)
		return ret;

	osd_execute_request(or);
	ret = osd_req_decode_sense(or, &osi);

	if (ret) { /* translate to Linux codes */
		if (osi.additional_code == scsi_invalid_field_in_cdb) {
			if (osi.cdb_field_offset == OSD_CFO_STARTING_BYTE)
				ret = 0; /*this is OK*/
			if (osi.cdb_field_offset == OSD_CFO_OBJECT_ID)
				ret = -ENOENT;
			else
				ret = -EINVAL;
		} else if (osi.additional_code == osd_quota_error)
			ret = -ENOSPC;
		else
			ret = -EIO;
	}

	return ret;
}

static int osdblk_exec_task(struct osd_request *or, u8 *cred, uint64_t *tid)
{
	struct osd_sense_info osi;
	int ret;

	ret = osd_finalize_request(or, 0, cred, NULL);
	if (ret)
		return ret;

	osd_execute_request(or);
	ret = osd_req_decode_sense(or, &osi);

	if (ret) { /* translate to Linux codes */
		if (osi.key == scsi_sk_vendor_specific &&
			osi.additional_code == osd_submitted_task_id)
		{
			//printf("task id = %llu\n", _LLU(osi.command_info));
			*tid = osi.command_info;
			ret = 0;

			goto out;
		}

		if (osi.additional_code == scsi_invalid_field_in_cdb) {
			if (osi.cdb_field_offset == OSD_CFO_STARTING_BYTE)
				ret = 0; /*this is OK*/
			if (osi.cdb_field_offset == OSD_CFO_OBJECT_ID)
				ret = -ENOENT;
			else
				ret = -EINVAL;
		} else if (osi.additional_code == osd_quota_error)
			ret = -ENOSPC;
		else
			ret = -EIO;
	}

out:
	return ret;
}

static int osd_execute_object_osdlib(struct osd_dev *osd,
				struct anfs_osd_request *req)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	struct anfs_task *task = req->task;
	struct osd_request *or = osd_start_request(osd, GFP_KERNEL);
	u32 len = task->argument ? strlen(task->argument) : 0;
	u64 input = task->input_cid;
	u64 output = task->output_cid;
	char *param_str = task->argument;
	struct osd_obj_id obj = {
		.partition = req->partition,
		.id = task->koid
	};
	uint64_t tid;

	if (unlikely(!or))
		return -ENOMEM;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));

	if (param_str)
		len = strlen(param_str);

	ret = osd_req_execute_kernel(or, &obj, input, output, len, param_str);
	if (ret)
		return ret;

	ret = osdblk_exec_task(or, creds, &tid);
	osd_end_request(or);

	task->tid = tid;

	return ret;
}

static int osd_query_task_osdlib(struct osd_dev *osd,
				struct anfs_osd_request *req,
				struct osd_active_task_status *st)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	struct osd_request *or = osd_start_request(osd, GFP_KERNEL);
	struct anfs_task *task = req->task;
	struct osd_obj_id obj = {
		.partition = 0,
		.id = task->tid
	};
	struct osd_active_task_status stbuf;

	if (unlikely(!or))
		return -ENOMEM;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));

	ret = osd_req_execute_query(or, &obj, &stbuf);
	if (ret)
		return ret;

	ret = osdblk_exec(or, creds);
	osd_end_request(or);

	/*
	 * adjust the byte ordering to host endian
	 */
	st->status = (unsigned long) be32toh(stbuf.status);
	st->ret = be32toh(stbuf.ret);
	st->submit = be64toh(stbuf.submit);
	st->start = be64toh(stbuf.start);
	st->complete = be64toh(stbuf.complete);

	return ret;
}
/**
 * read the id from the /sys/fs/exofs/osdX/sync_id.
 * on error returns 0.
 */
static uint64_t get_exofs_sync_id(struct osd_dev *osd, int dev)
{
	int ret;
	FILE *fp;
	char sync_path[32];
	uint64_t id;

	sprintf(sync_path, "/sys/fs/exofs/osd%d/sync_id", dev);
	fp = fopen(sync_path, "r");
	if (!fp)
		return 0;

	if (NULL == fgets(sync_path, 31, fp)) {
		id = 0;
		goto out;
	}

	sscanf(sync_path, "%lu", &id);
out:
	fclose(fp);

	return id;
}


/**
 * worker thread implementation.
 */

static int terminate_workers(struct anfs_osd *self)
{
	int i, ret;
	void *res;
	pthread_t id;

	for (i = 0; i < self->ndev+1; i++) {
		pthread_mutex_destroy(&self->workers[i].lock);
		id = self->workers[i].id;
		ret = pthread_cancel(id);
	}

	for (i = 0; i < self->ndev+1; i++) {
		id = self->workers[i].id;
		ret = pthread_join(id, &res);
	}

	return ret;
}

static inline void lock_rq(struct anfs_osd_worker *worker)
{
	pthread_mutex_lock(&worker->lock);
}

static inline void unlock_rq(struct anfs_osd_worker *worker)
{
	pthread_mutex_unlock(&worker->lock);
}

static inline int worker_execute_object(struct anfs_osd_worker *wd,
					struct anfs_osd_request *req)
{
	int ret = osd_execute_object_osdlib(wd->osd, req);

	if (!ret) {
		lock_rq(wd->checker);
		list_add_tail(&req->list, &wd->checker->rq);
		unlock_rq(wd->checker);
	}

	return ret;
}

/**
 * fetch_request fetches a request from the private request queue.
 *
 * @wd: thread work data.
 *
 * returns a request fetched, if any. NULL otherwise.
 */
static inline
struct anfs_osd_request *fetch_request(struct anfs_osd_worker *wd)
{
	struct anfs_osd_request *req = NULL;

	lock_rq(wd);
	if (list_empty(&wd->rq))
		goto out;

	req = list_first_entry(&wd->rq, struct anfs_osd_request, list);
	list_del(&req->list);

out:
	unlock_rq(wd);
	return req;
}

static void *anfs_osd_worker_func(void *arg)
{
	int ret = 0;
	struct anfs_osd_worker *data = (struct anfs_osd_worker *) arg;
	struct anfs_osd_request *req;

	while (1) {
		req = fetch_request(data);

		if (!req) {
			usleep(data->idle_sleep);
			continue;
		}

		if (req->dev != data->dev) {
			/** XXX: bug!! do something! */
			ret = -1;
			break;
		}

		switch (req->type) {
		case ANFS_OSD_RQ_SETATTR:
			return NULL;

		case ANFS_OSD_RQ_GETATTR:
			return NULL;

		case ANFS_OSD_RQ_EXECUTE:
			data->stat.n_execute++;
			ret = worker_execute_object(data, req);
			break;

		default:
			break;
		}

		/**
		 * task execution should be handled asynchronously
		 */
		if (req->type != ANFS_OSD_RQ_EXECUTE) {
			req->status = ret;
			req->t_complete = anfs_now();
			if (req->callback)
				(*req->callback) (ret, req);
		}
	}

	return (void *) ((unsigned long) ret);
}

static void *anfs_osd_worker_task_checker(void *arg)
{
	int ret = 0;
	struct anfs_osd_worker *data = (struct anfs_osd_worker *) arg;
	struct anfs_osd_request *req;
	struct anfs_task *task;
	struct osd_active_task_status status;
	struct osd_dev *osd;

	while (1) {
		req = fetch_request(data);

		if (!req) {
			usleep(data->idle_sleep);
			continue;
		}

		memset((void *) &status, 0, sizeof(status));
		osd = req->osd;

		/** query whether it is finished */
		memset((void *) &status, 0, sizeof(status));
		ret = osd_query_task_osdlib(osd, req, &status);

		if (ret == 0 && status.status == 2) {	/** task completed */
			task = req->task;
			task->ret = status.ret;
			task->t_submit = status.submit;
			task->t_start = status.start;
			task->t_complete = status.complete;

			req->t_complete = anfs_now();

			if (req->callback)
				(*req->callback) (task->ret, req);

			continue;
		}

		/** rest of the cases, try again. */

		lock_rq(data);
		list_add_tail(&req->list, &data->rq);
		unlock_rq(data);

#if 0
		if (ret || status.status != 2) {
			/**
			 * put it back to the queue.
			 * XXX: we need to distinguish cases here.
			 */
			lock_rq(data);
			list_add_tail(&req->list, &data->rq);
			unlock_rq(data);

			continue;
		}

		task = req->task;

		/** set status status */
		task->ret = status.ret;
		task->t_submit = status.submit;
		task->t_complete = status.complete;

		/** execute the caller's callback */
		if (req->callback)
			(*req->callback) (task->ret, req);
#endif
	}

	return (void *) ((unsigned long) ret);
}

static int set_devpaths(struct anfs_osd *self, int ndev, char **mntpnts)
{
	int i, done = 0;
	FILE *fp;
	char *pos, *mnt;
	char linebuf[512];

	if ((fp = fopen("/etc/mtab", "r")) == NULL)
		return -errno;

	while (fgets(linebuf, 511, fp) != NULL) {
		for (i = 0; i < ndev; i++) {
			mnt = mntpnts[i];
			if (NULL == (pos = strstr(linebuf, mnt)))
				continue;

			/* dev comes at the very beginning of the line */
			pos = strchr(linebuf, ' ');
			*pos = '\0';
			self->devpaths[done++] = strdup(linebuf);
		}
	}

	fclose(fp);
	if (ferror(fp))
		return -errno;

	return done == ndev ? 0 : -EINVAL;
}

/**
 * external interface of osd component.
 */

/** TODO: fix the cleanup process (goto/free) */
int anfs_osd_init(struct anfs_osd *self, int ndev, char **devpaths,
			uint64_t idle_sleep)
{
	int i, ret = 0;
	struct anfs_osd_worker *current, *workers;
	struct anfs_ctx *ctx = anfs_ctx(self, osd);

	if (ndev <= 0)
		return -EINVAL;

	workers = calloc(ndev+1, sizeof(*workers));
	if (!workers)
		return -ENOMEM;

	self->partition = anfs_config(ctx)->partition;
	self->ndev = ndev;
	self->devpaths = calloc(ndev, sizeof(char *));
	if (!self->devpaths) {
		free(workers);
		return -ENOMEM;
	}

	ret = set_devpaths(self, ndev, devpaths);
	if (ret) {
		free(self->devpaths);
		free(workers);
		return -EINVAL;
	}

	for (i = 0; i < ndev; i++) {
		current = &workers[i];

		ret = osd_open(self->devpaths[i], &current->osd);
		if (ret)
			goto out_clean;

		current->dev = i;
		current->idle_sleep = idle_sleep;

		pthread_mutex_init(&current->lock, NULL);

		INIT_LIST_HEAD(&current->rq);
		ret = pthread_create(&current->id, NULL, &anfs_osd_worker_func,
				current);
		if (ret) {
			ret = -errno;
			goto out_clean;
		}
	}

	self->workers = workers;

	/** initialize the checker */
	current = &workers[i];
	memset(current, 0, sizeof(*current));
	current->idle_sleep = idle_sleep;

	pthread_mutex_init(&current->lock, NULL);

	INIT_LIST_HEAD(&current->rq);
	ret = pthread_create(&current->id, NULL, &anfs_osd_worker_task_checker,
				current);
	if (ret) {
		ret = -errno;
		goto out_clean;
	}

	self->checker = current;

	for (i = 0; i < ndev; i++) {
		current = &workers[i];
		current->checker = self->checker;
	}

	return 0;

out_clean:
	/** XXX: is this a correct way to clean up? */
	for ( ; i >= 0; i++) {
		pthread_mutex_destroy(&workers[i].lock);
		pthread_kill(workers[i].id, SIGKILL);
	}

	free(self->devpaths);
	free(self->workers);

	return ret;
}

void anfs_osd_exit(struct anfs_osd *self)
{
	int i;

	if (self && self->workers) {
		for (i = 0; i < self->ndev; i++)
			osd_close(self->workers[i].osd);

		terminate_workers(self);
		free(self->workers);
	}
}

int anfs_osd_submit_request(struct anfs_osd *self,
				struct anfs_osd_request *req)
{
	struct anfs_osd_worker *worker;

	if (!self || !req)
		return -EINVAL;
	if (self->ndev <= req->dev)
		return -EINVAL;

	worker = &self->workers[req->dev];

	req->t_submit = anfs_now();
	req->osd = worker->osd;
	req->partition = self->partition;

	lock_rq(worker);
	list_add_tail(&req->list, &worker->rq);
	unlock_rq(worker);

	return 0;
}

int anfs_osd_create_collection(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t *cid)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	uint64_t tmp;
	struct osd_dev *osd = self->workers[dev].osd;
	struct osd_request *or = osd_start_request(osd, GFP_KERNEL);
	struct osd_obj_id obj = { .partition = pid, .id = 0 };

	if (unlikely(!or))
		return -ENOMEM;

	if (!cid)
		return -EINVAL;

	if (0 == (tmp = get_exofs_sync_id(osd, dev)))
		return -EIO;

	obj.id = *cid = tmp + ANFS_OBJECT_OFFSET;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_create_collection(or, &obj);

	ret = osdblk_exec(or, creds);
	osd_end_request(or);

	return ret;
}

int anfs_osd_set_membership(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t cid, uint64_t *objs, uint32_t len)
{
	int i, ret = 0;
	u8 creds[OSD_CAP_LEN];
	struct osd_obj_id obj;
	__be64 be_cid = cpu_to_be64(cid);
	struct osd_request *or;
	struct osd_dev *osd = self->workers[dev].osd;
	struct osd_attr membership = ATTR_SET(OSD_APAGE_OBJECT_COLLECTIONS, 1,
						sizeof(be_cid), &be_cid);
	obj.partition = pid;

	for (i = 0; i < len; i++) {
		or = osd_start_request(osd, GFP_KERNEL);
		if (unlikely(!or))
			return -ENOMEM;

		obj.id = objs[i];

		osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));

		osd_req_set_attributes(or, &obj);
		osd_req_add_set_attr_list(or, &membership, 1);

		ret = osdblk_exec(or, creds);
		osd_end_request(or);

		if (ret)
			break;
	}

	return ret;
}

const struct osd_attr g_attr_logical_length = ATTR_DEF(
		OSD_APAGE_OBJECT_INFORMATION, OSD_ATTR_OI_LOGICAL_LENGTH, 8);

int anfs_osd_get_object_size(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t oid, uint64_t *size)
{
	int ret = 0;
	u8 creds[OSD_CAP_LEN];
	struct osd_dev *osd = self->workers[dev].osd;
	struct osd_request *or = osd_start_request(osd, GFP_KERNEL);
	struct osd_obj_id obj = {
		.partition = pid,
		.id = oid
	};
	int nelem = 1;
	void *iter = NULL;
	struct osd_attr attr;

	if (unlikely(!or))
		return -ENOMEM;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_get_attributes(or, &obj);
	osd_req_add_get_attr_list(or, &g_attr_logical_length, 1);

	ret = osdblk_exec(or, creds);

	if (!ret) {
		osd_req_decode_get_attr_list(or, &attr, &nelem, &iter);
		*size = get_unaligned_be64(attr.val_ptr);
	}

	osd_end_request(or);

	return ret;
}

