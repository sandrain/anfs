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

static const uint64_t AFS_OBJECT_OFFSET = 0x10000;

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

static int osd_create_object_osdlib(struct anfs_osd_dev *aosd, uint64_t oid)
{
	int ret = 0;
	u8 creds[OSD_CAP_LEN];
	/** XXX: fix this. currently hard coded */
	struct osd_obj_id obj = { .partition = 0x22222,
				.id = AFS_OBJECT_OFFSET+oid };
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);

	if (unlikely(!or))
		return -ENOMEM;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_create_object(or, &obj);
	ret = osdblk_exec(or, creds);
	osd_end_request(or);

	return ret;
}

static int osd_remove_object_osdlib(struct anfs_osd_dev *aosd, uint64_t oid)
{
	int ret = 0;
	u8 creds[OSD_CAP_LEN];
	struct osd_obj_id obj = { .partition = 0x22222,
				.id = AFS_OBJECT_OFFSET+oid };
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);

	if (unlikely(!or))
		return -ENOMEM;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_remove_object(or, &obj);
	ret = osdblk_exec(or, creds);
	osd_end_request(or);

	return ret;
}

static int osd_write_object_osdlib(struct anfs_osd_dev *aosd, uint64_t oid,
				const void *data, size_t count, off_t offset)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	struct osd_obj_id obj = { .partition = 0x22222,
				.id = AFS_OBJECT_OFFSET+oid };
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);

	if (unlikely(!or))
		return -ENOMEM;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_write_kern(or, &obj, offset, (void *) data, count);
	ret = osdblk_exec(or, creds);
	osd_end_request(or);

	return ret;
}

static int osd_read_object_osdlib(struct anfs_osd_dev *aosd, uint64_t oid,
				void *data, size_t count, off_t offset)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	struct osd_obj_id obj = { .partition = 0x22222,
				.id = AFS_OBJECT_OFFSET+oid };
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);

	if (unlikely(!or))
		return -ENOMEM;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_read_kern(or, &obj, offset, data, count);
	ret = osdblk_exec(or, creds);
	osd_end_request(or);

	return ret;
}

static int osd_dsync_object_osdlib(struct anfs_osd_dev *aosd, uint64_t oid)
{
	return 0;
}

static int osd_execute_object_osdlib(struct anfs_osd_dev *aosd,
				struct anfs_osd_request *req)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	struct anfs_task *task = req->task;
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);
	u32 len = task->argument ? strlen(task->argument) : 0;
	u64 input = task->input_cid + AFS_OBJECT_OFFSET;
	u64 output = task->output_cid + AFS_OBJECT_OFFSET;
	char *param_str = task->argument;
	struct osd_obj_id obj = {
		.partition = 0x22222,
		.id = task->koid + AFS_OBJECT_OFFSET
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

static int osd_query_task_osdlib(struct anfs_osd_dev *aosd,
				struct anfs_osd_request *req,
				struct osd_active_task_status *st)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);
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

static int osd_create_collection_osdlib(struct anfs_osd_dev *aosd,
					uint64_t pid, uint64_t *cid)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	int nelem = 1;
	void *iter = NULL;
	struct osd_attr attr;
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);
	struct osd_obj_id obj = { .partition = pid, .id = 0 };

	if (unlikely(!or))
		return -ENOMEM;

	if (!cid) {
		ret = -EINVAL;
		goto out;
	}

	if (*cid)
		obj.id = *cid + AFS_OBJECT_OFFSET;

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_create_collection(or, &obj);

	if (*cid == 0) {
		/** we need to retrieve the collection id allocated */
		attr.attr_page = OSD_APAGE_CURRENT_COMMAND;
		attr.attr_id = OSD_APAGE_OBJECT_COLLECTIONS;
		attr.len = sizeof(__be64);
		attr.val_ptr = NULL;
		ret = osd_req_add_get_attr_list(or, &attr, 1);
	}

	ret = osdblk_exec(or, creds);
	if (ret)
		goto out;

	if (*cid == 0) {
		osd_req_decode_get_attr_list(or, &attr, &nelem, &iter);
		*cid = get_unaligned_be64(attr.val_ptr);
	}

out:
	osd_end_request(or);

	return ret;
}

/**
 * do we really to need to submit a request for each object??
 */
static int osd_set_membership_osdlib(struct anfs_osd_dev *aosd, uint64_t pid,
				uint64_t cid, uint64_t *objs, uint32_t len)
{
	int i, ret = 0;
	u8 creds[OSD_CAP_LEN];
	struct osd_obj_id obj;
	__be64 be_cid = cpu_to_be64(cid + AFS_OBJECT_OFFSET);
	struct osd_request *or;
	struct osd_attr membership = ATTR_SET(OSD_APAGE_OBJECT_COLLECTIONS, 1,
						sizeof(be_cid), &be_cid);
	obj.partition = pid;

	for (i = 0; i < len; i++) {
		or = osd_start_request(aosd->osd, GFP_KERNEL);
		if (unlikely(!or))
			return -ENOMEM;

		obj.id = objs[i] + AFS_OBJECT_OFFSET;

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

static int osd_get_object_size_osdlib(struct anfs_osd_dev *aosd, uint64_t pid,
				uint64_t oid, uint64_t *size)
{
	int ret = 0;
	u8 creds[OSD_CAP_LEN];
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);
	struct osd_obj_id obj = {
		.partition = pid,
		.id = oid + AFS_OBJECT_OFFSET
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

/**
 * using exofs for fast prototyping. if performance sucks, implement and use
 * the direct osd interface.
 *
 * hs: it turned out to be that direct osd implementation exhibits horrible
 * performance due to the limitation of the user-level osd library (all I/O
 * requests are serialized).
 * we are going back to the exofs backend, which will involve several changes
 * to the exofs as well; especially, the id conflict should be resolved
 * because we also allocate the object id for the collections.
 */

static inline void get_exofs_path(struct anfs_osd_dev *aosd,
					uint64_t oid, char *buf)
{
	sprintf(buf, "%s/objects/%02x/%016llx.af",
			aosd->mnt, (uint8_t) (oid & 0xffUL), anfs_llu(oid));
}

static inline void get_exofs_path_worker(struct anfs_osd_dev *aosd, uint64_t oid)
{
	get_exofs_path(aosd, oid, aosd->nbuf);
}

static int osd_create_object_exofs(struct anfs_osd_dev *aosd, uint64_t oid)
{
	int ret;

	get_exofs_path_worker(aosd, oid);

	ret = open(aosd->nbuf, O_CREAT|O_WRONLY|O_EXCL|O_TRUNC);
	if (ret == -1)
		return ret;

	return close(ret);
}

static int osd_remove_object_exofs(struct anfs_osd_dev *aosd, uint64_t oid)
{
	get_exofs_path_worker(aosd, oid);

	return unlink(aosd->nbuf);
}

/** XXX; is it find to use the buffered write here? */

static int osd_write_object_exofs(struct anfs_osd_dev *aosd, uint64_t oid,
				const void *data, size_t count, off_t offset)
{
	int ret;
	FILE *fp;
	size_t n_written;

	get_exofs_path_worker(aosd, oid);

	fp = fopen(aosd->nbuf, "w");
	if (!fp) {
		ret = -errno;
		goto out;
	}

	ret = fseek(fp, offset, SEEK_SET);
	if (ret == -1) {
		ret = -errno;
		goto out_close;
	}

	n_written = fwrite(data, count, 1, fp);
	if (n_written != 1) {	/* in write, eof should not be a problem */
		ret = -errno;
		goto out_close;
	}

	ret = 0;

out_close:
	fclose(fp);
out:
	return ret;
}

static int osd_read_object_exofs(struct anfs_osd_dev *aosd, uint64_t oid,
				void *data, size_t count, off_t offset)
{
	int ret;
	FILE *fp;
	size_t n_read;

	get_exofs_path_worker(aosd, oid);

	fp = fopen(aosd->nbuf, "r");
	if (!fp) {
		ret = -errno;
		goto out;
	}

	ret = fseek(fp, offset, SEEK_SET);
	if (ret == -1) {
		ret = -errno;
		goto out_close;
	}

	n_read = fread(data, count, 1, fp);

	/** error caused by reaching eof is considered success.
	 * XXX; how the caller happen to know about this??
	 */
	if (n_read != 1)
		ret = feof(fp) ? 0 : -errno;
	else
		ret = 0;

out_close:
	fclose(fp);
out:
	return ret;
}

static int osd_dsync_object_exofs(struct anfs_osd_dev *aosd, uint64_t oid)
{
	int ret = 0;
	int fd;

	get_exofs_path_worker(aosd, oid);

	fd = open(aosd->nbuf, O_RDWR);
	if (fd < 0) {
		ret = -errno;
		goto out;
	}

	ret = fsync(fd);
	close(fd);
out:
	return ret;
}

/**
 * read the id from the /sys/fs/exofs/osdX/sync_id.
 */
static uint64_t get_exofs_sync_id(struct anfs_osd_dev *aosd, int dev)
{
	FILE *fp;
	char sync_path[32];
	uint64_t id;

	sprintf(sync_path, "/sys/fs/exofs/osd%d/sync_id", dev);
	fp = fopen(sync_path, "r");
	if (!fp)
		return 0;

	if (1 != fread((void *) &id, sizeof(id), 1, fp))
		id = 0;

	fclose(fp);

	return id;
}

static int osd_create_collection_exofs(struct anfs_osd_dev *aosd, int dev,
					uint64_t pid, uint64_t *cid)
{
	int ret;
	u8 creds[OSD_CAP_LEN];
	struct osd_request *or = osd_start_request(aosd->osd, GFP_KERNEL);
	struct osd_obj_id obj = { .partition = pid, .id = 0 };

	if (unlikely(!or))
		return -ENOMEM;

	if (!cid) {
		return -EINVAL;
	}

	if (*cid)
		obj.id = *cid + AFS_OBJECT_OFFSET;
	else {
		*cid = obj.id = get_exofs_sync_id(aosd, dev);
		if (0 == *cid)
			return -EIO;
	}

	osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
	osd_req_create_collection(or, &obj);

	ret = osdblk_exec(or, creds);
	osd_end_request(or);

	return ret;
}

static int osd_execute_object_exofs(struct anfs_osd_dev *aosd,
				struct anfs_osd_request *req)
{
	return -ENOSYS;
}

static int osd_get_object_size_exofs(struct anfs_osd_dev *aosd, uint64_t pid,
				uint64_t oid, uint64_t *size)
{
	int ret;
	struct stat stbuf;

	get_exofs_path_worker(aosd, oid);
	ret = stat(aosd->nbuf, &stbuf);
	if (ret == -1)
		return -errno;

	*size = stbuf.st_size;

	return 0;
}

/**
 * worker thread implementation.
 */

static int terminate_workers(struct anfs_osd *self)
{
	int i, ret;
	void *res;
	pthread_t id;

	for (i = 0; i < self->ndev+2; i++) {
		pthread_mutex_destroy(&self->workers[i].lock);
		id = self->workers[i].id;
		ret = pthread_cancel(id);
	}

	for (i = 0; i < self->ndev+2; i++) {
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

static inline int worker_create_object(struct anfs_osd_worker *wd,
				struct anfs_osd_request *req)
{
	if (wd->direct)
		return osd_create_object_osdlib(&wd->osd, req->ino);
	else
		return osd_create_object_exofs(&wd->osd, req->ino);
}

static inline int worker_remove_object(struct anfs_osd_worker *wd,
				struct anfs_osd_request *req)
{
	if (wd->direct)
		return osd_remove_object_osdlib(&wd->osd, req->ino);
	else
		return osd_remove_object_exofs(&wd->osd, req->ino);
}

static inline int worker_read_object(struct anfs_osd_worker *wd,
				struct anfs_osd_request *req)
{
	if (wd->direct)
		return osd_read_object_osdlib(&wd->osd, req->ino, req->buf,
						req->size, req->off);
	else
		return osd_read_object_exofs(&wd->osd, req->ino, req->buf,
						req->size, req->off);
}

static inline int worker_write_object(struct anfs_osd_worker *wd,
				struct anfs_osd_request *req)
{
	if (wd->direct)
		return osd_write_object_osdlib(&wd->osd, req->ino, req->buf,
						req->size, req->off);
	else
		return osd_write_object_exofs(&wd->osd, req->ino, req->buf,
						req->size, req->off);
}

static inline int worker_sync_object(struct anfs_osd_worker *wd,
				struct anfs_osd_request *req)
{
	if (wd->direct)
		return osd_dsync_object_osdlib(&wd->osd, req->ino);
	else
		return osd_dsync_object_exofs(&wd->osd, req->ino);
}

static inline int worker_setattr_object(struct anfs_osd_worker *wd,
					struct anfs_osd_request *req)
{
	return -1;
}

static inline int worker_getattr_object(struct anfs_osd_worker *wd,
					struct anfs_osd_request *req)
{
	return -1;
}

static inline int worker_execute_object(struct anfs_osd_worker *wd,
					struct anfs_osd_request *req)
{
	int ret;

	if (wd->direct)
		ret = osd_execute_object_osdlib(&wd->osd, req);
	else
		return osd_execute_object_exofs(&wd->osd, req);

	/** only direct mode falls here */
	if (!ret) {
		lock_rq(wd->checker);
		list_add_tail(&req->list, &wd->checker->rq);
		unlock_rq(wd->checker);
	}

	return ret;
}

static inline int worker_create_collection(struct anfs_osd_worker *wd,
					struct anfs_osd_request *req)
{
	if (wd->direct)
		return osd_create_collection_osdlib(&wd->osd, req->ino,
						&req->cid);
	else
		return -ENOSYS;
}

static inline int worker_set_membership(struct anfs_osd_worker *wd,
					struct anfs_osd_request *req)
{
	if (wd->direct)
		return osd_set_membership_osdlib(&wd->osd, 0x22222, req->ino,
						(uint64_t *) req->buf,
						(uint32_t) req->size);
	else
		return -ENOSYS;
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
		case AFS_OSD_RQ_CREATE:
			data->stat.n_create++;
			ret = worker_create_object(data, req);
			break;

		case AFS_OSD_RQ_REMOVE:
			data->stat.n_remove++;
			ret = worker_remove_object(data, req);
			break;

		case AFS_OSD_RQ_READ:
			data->stat.n_read++;
			ret = worker_read_object(data, req);
			break;

		case AFS_OSD_RQ_WRITE:
			data->stat.n_write++;
			ret = worker_write_object(data, req);
			break;

		case AFS_OSD_RQ_DSYNC:
			data->stat.n_dsync++;
			ret = worker_sync_object(data, req);
			break;

		case AFS_OSD_RQ_SETATTR:
			data->stat.n_setattr++;
			ret = worker_setattr_object(data, req);
			break;

		case AFS_OSD_RQ_GETATTR:
			data->stat.n_getattr++;
			ret = worker_getattr_object(data, req);
			break;

		case AFS_OSD_RQ_EXECUTE:
			data->stat.n_execute++;
			ret = worker_execute_object(data, req);
			break;

		case AFS_OSD_RQ_REPLICATE:
			break;

#if 0
		case AFS_OSD_RQ_CREAT_COLLECTION:
			ret = worker_create_collection(data, req);
			break;

		case AFS_OSD_RQ_SET_MEMBERSHIP:
			ret = worker_set_membership(data, req);
			break;
#endif

		default:
			break;
		}

		/**
		 * task execution should be handled asynchronously
		 */
		if (req->type != AFS_OSD_RQ_EXECUTE) {
			req->status = ret;
			req->t_complete = anfs_now();
			if (req->callback)
				(*req->callback) (ret, req);
		}
	}

	return (void *) ((unsigned long) ret);
}

/**
 * object copy implementation.
 *
 * the most convincing way to copy a file between devices is to use external
 * 'cp' program via fork or execl, which cannot be an option here because:
 * . spawning a process from multi-threaded application is complicated.
 * . for direct osdlib, we cannot use the 'cp' anyway.
 *
 * so we just do the naive copy.
 */
#define	AFS_OSD_COPIER_BUFSIZE		(1<<20)
static char copier_buffer[AFS_OSD_COPIER_BUFSIZE];
static struct anfs_osd_dev **copier_devs;

static int copier_replicate_object_osdlib(uint64_t ino, int src_dev, int dst_dev)
{
#ifdef	__AFS_OSD_DIRECT_TRANSFER__
	/** may come later.. */
#else
	int ret;
	int exit = 0;
	struct osd_request *ors, *ord;
	u8 credss[OSD_CAP_LEN], credsd[OSD_CAP_LEN];
	u64 total_read = 0, n_read = 0;
	u64 total_write = 0, n_written = 0;
	struct osd_sense_info osi;
	struct osd_dev *ods = copier_devs[src_dev]->osd;
	struct osd_dev *odd = copier_devs[dst_dev]->osd;
	struct osd_obj_id common_obj = {
		.partition = 0x22222,
		.id = ino,
	};
	struct osd_obj_id *src, *dst;

	src = dst = &common_obj;

	/**
	 * we might need to create the dest. obj.
	 */
	ret = osd_create_object_osdlib(copier_devs[dst_dev], common_obj.id);
	if (ret) {
		/** ignore, maybe object already exist. */
	}

	/** ugly, we have to fix this offset thing */
	common_obj.id += AFS_OBJECT_OFFSET;

	while (1) {
		ors = osd_start_request(ods, GFP_KERNEL);
		ord = osd_start_request(odd, GFP_KERNEL);
		if (!(ors && ord))
			return -ENOMEM;

		osdblk_make_credential(credss, src, osd_req_is_ver1(ors));
		osdblk_make_credential(credsd, dst, osd_req_is_ver1(ord));

		/**
		 * read the source
		 */
		ret = osd_req_read_kern(ors, src, total_read, copier_buffer,
					AFS_OSD_COPIER_BUFSIZE);
		if (ret) {
			osd_end_request(ors);
			return ret;
		}

		ret = osd_finalize_request(ors, 0, credss, NULL);
		if (ret) {
			osd_end_request(ors);
			return ret;
		}

		ret = osd_execute_request(ors);
		if (ret) {
			ret = osd_req_decode_sense(ors, &osi);
			if (osi.additional_code == osd_read_past_end_of_user_object)
			{
				n_read = osi.command_info;
				exit = 1;
			}
		}
		else
			n_read = AFS_OSD_COPIER_BUFSIZE;

		total_read += n_read;
		osd_end_request(ors);

		if (n_read == 0)
			break;	/** no more write necessary */

		/**
		 * write the dest
		 */
#if 0
		ret = osd_req_write_kern(ord, dst, total_write, copier_buffer,
					AFS_OSD_COPIER_BUFSIZE);
#endif
		ret = osd_req_write_kern(ord, dst, total_write, copier_buffer,
					n_read);
		if (ret) {
			osd_end_request(ord);
			return ret;
		}

		ret = osd_finalize_request(ord, 0, credsd, NULL);
		if (ret) {
			osd_end_request(ord);
			return ret;
		}

		ret = osd_execute_request(ord);
		if (!ret)
			goto next;

		ret = osd_req_decode_sense(ord, &osi);
		if (ret) {
			if (osi.additional_code == scsi_invalid_field_in_cdb) {
				if (osi.cdb_field_offset == OSD_CFO_STARTING_BYTE)
					ret = 0; /*this is OK*/
				else if (osi.cdb_field_offset == OSD_CFO_OBJECT_ID)
					ret = -ENOENT;
				else
					ret = -EINVAL;
			} else if (osi.additional_code == osd_quota_error)
				ret = -ENOSPC;
			else
				ret = -EIO;

			if (ret) {
				osd_end_request(ord);
				return ret;
			}
		}

next:
		osd_end_request(ord);

		n_written = n_read;
		total_write += n_written;
		if (exit)
			break;
	}

	return ret;
#endif
}

static int copier_replicate_object_exofs(uint64_t ino, int src, int dest)
{
	int ret;
	int fd_to, fd_from;
	char *fin = copier_buffer;
	char *fout = &copier_buffer[128];
	ssize_t n_read;

	get_exofs_path(copier_devs[src], ino, fin);
	get_exofs_path(copier_devs[dest], ino, fout);

	fd_from = open(fin, O_RDONLY);
	if (fd_from < 0)
		return -errno;
	fd_to = open(fout, O_WRONLY|O_CREAT|O_EXCL, 0666);
	if (fd_to < 0) {
		ret = -errno;
		goto out_err;
	}

	while ((n_read =
		read(fd_from, copier_buffer, AFS_OSD_COPIER_BUFSIZE)) > 0)
	{
		char *out_ptr = copier_buffer;
		ssize_t n_written;

		do {
			n_written = write(fd_to, out_ptr, n_read);

			if (n_written >= 0) {
				n_read -= n_written;
				out_ptr += n_written;
			}
			else if (errno != EINTR) {
				ret = -errno;
				goto out_err;
			}
		} while (n_read > 0);
	}

	if (n_read == 0)
		ret = 0;

out_err:
	close(fd_from);
	if (fd_to >= 0)
		close(fd_to);

	return ret;
}

static void *anfs_osd_worker_copier(void *arg)
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

		if (req->type != AFS_OSD_RQ_REPLICATE) {
			/** TODO: error!! */
			usleep(data->idle_sleep);
			continue;
		}

		data->stat.n_replicate++;

		if (data->direct)
			ret = copier_replicate_object_osdlib(req->ino,
						req->dev, req->destdev);
		else
			ret = copier_replicate_object_exofs(req->ino,
						req->dev, req->destdev);

		req->status = ret;
		req->t_complete = anfs_now();
		if (req->callback)
			(*req->callback) (ret, req);
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
	struct anfs_osd_dev *aosd;

	while (1) {
		req = fetch_request(data);

		if (!req) {
			usleep(data->idle_sleep);
			continue;
		}

		memset((void *) &status, 0, sizeof(status));

		aosd = copier_devs[req->dev];

		/** query whether it is finished */
		memset((void *) &status, 0, sizeof(status));
		ret = osd_query_task_osdlib(aosd, req, &status);

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

/**
 * external interface of osd component.
 */

int anfs_osd_init(struct anfs_osd *self, int ndev, char **devpaths, int direct,
		uint64_t idle_sleep)
{
	int i, ret = 0;
	struct anfs_osd_worker *current, *workers;

	if (ndev <= 0)
		return -EINVAL;

	workers = calloc(ndev+2, sizeof(*workers));
	if (!workers)
		return -ENOMEM;
	copier_devs = calloc(ndev, sizeof(*copier_devs));
	if (!copier_devs) {
		free(workers);
		return -ENOMEM;
	}

	self->ndev = ndev;
	self->direct = direct;

	for (i = 0; i < ndev; i++) {
		current = &workers[i];

		/** the direct interface is not yet implemented. */
		current->direct = direct;
		if (direct) {
			/**
			 * TODO: open the osd device and store the handle
			 */

			ret = osd_open(devpaths[i], &current->osd.osd);
			if (ret)
				goto out_clean;
			current->osd.mnt = NULL;
		}
		else {
			current->osd.osd = NULL;
			current->osd.mnt = strdup(devpaths[i]);
		}

		current->dev = i;
		current->idle_sleep = idle_sleep;

		copier_devs[i] = &current->osd;	/** grant access to copier */

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

	/** initialize the copier */
	current = &workers[i++];
	memset(current, 0, sizeof(*current));
	current->direct = direct;
	current->idle_sleep = idle_sleep;

	pthread_mutex_init(&current->lock, NULL);

	INIT_LIST_HEAD(&current->rq);
	ret = pthread_create(&current->id, NULL, &anfs_osd_worker_copier,
				current);
	if (ret) {
		ret = -errno;
		goto out_clean;
	}

	self->copier = current;

	/** initialize the checker */
	current = &workers[i];
	memset(current, 0, sizeof(*current));
	current->direct = direct;
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
		current->copier = self->copier;
		current->checker = self->checker;
	}

	return 0;

out_clean:
	/** XXX: is this a correct way to clean up? */
	for ( ; i >= 0; i++) {
		pthread_mutex_destroy(&workers[i].lock);
		pthread_kill(workers[i].id, SIGKILL);
	}

	return ret;
}

void anfs_osd_exit(struct anfs_osd *self)
{
	int i;

	if (self && self->workers) {
		if (self->direct)
			for (i = 0; i < self->ndev; i++)
				osd_close(self->workers[i].osd.osd);
		else
			for (i = 0; i < self->ndev; i++)
				free(self->workers[i].osd.mnt);

		terminate_workers(self);
		free(self->workers);
	}

	if (copier_devs)
		free(copier_devs);
}

int anfs_osd_submit_request(struct anfs_osd *self, struct anfs_osd_request *req)
{
	struct anfs_osd_worker *worker;

	if (!self || !req)
		return -EINVAL;
	if (self->ndev <= req->dev)
		return -EINVAL;

	if (req->type == AFS_OSD_RQ_REPLICATE)
		worker = self->copier;
	else
		worker = &self->workers[req->dev];

	req->t_submit = anfs_now();

	lock_rq(worker);
	list_add_tail(&req->list, &worker->rq);
	unlock_rq(worker);

	return 0;
}

int anfs_osd_create_collection(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t *cid)
{
	struct anfs_osd_dev *aosd = &self->workers[dev].osd;

	if (self->workers[dev].direct)
		return osd_create_collection_osdlib(aosd, pid, cid);
	else
		return osd_create_collection_exofs(aosd, dev, pid, cid);
}

int anfs_osd_set_membership(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t cid, uint64_t *objs, uint32_t len)
{
	struct anfs_osd_dev *aosd = &self->workers[dev].osd;
	return osd_set_membership_osdlib(aosd, pid, cid, objs, len);
}

int anfs_osd_get_file_size(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t oid, uint64_t *size)
{
	struct anfs_osd_dev *aosd = &self->workers[dev].osd;

	if (self->workers[dev].direct)
		return osd_get_object_size_osdlib(aosd, pid, oid, size);
	else
		return osd_get_object_size_exofs(aosd, pid, oid, size);
}

