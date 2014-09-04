/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * The osd wrapping interface for internal use.
 */
#ifndef	__AFS_OSD_H__
#define	__AFS_OSD_H__

#include <unistd.h>
#include <pthread.h>
#include <open-osd/libosd.h>

struct anfs_osd_dev {
	struct osd_dev *osd;	/* osd device */
	char *mnt;		/* mount path (exofs),
				 * NULL if we use the direct osd library */
	char nbuf[64];
};

/**
 * osd request job type.
 */
enum {
	AFS_OSD_RQ_UNKNOWN	= 0,
	AFS_OSD_RQ_CREATE,	/* create an object */
	AFS_OSD_RQ_REMOVE,	/* remove an object */
	AFS_OSD_RQ_READ,	/* read data from an object */
	AFS_OSD_RQ_WRITE,	/* write data to an object */
	AFS_OSD_RQ_DSYNC,	/* sync data (on fsync) */
	AFS_OSD_RQ_SETATTR,	/* set attribute of an object */
	AFS_OSD_RQ_GETATTR,	/* get attribute of an object */
	AFS_OSD_RQ_EXECUTE,	/* submit an active task */
	AFS_OSD_RQ_REPLICATE,	/* replicate a file into another device */
	AFS_OSD_RQ_CREAT_COLLECTION,	/* create an collection */
	AFS_OSD_RQ_SET_MEMBERSHIP,	/* associate objects to a collection */
};

struct anfs_osd_tasklet {
};

/**
 * request statistics.
 */
struct anfs_osd_rstat {
	uint64_t n_create;
	uint64_t n_remove;
	uint64_t n_read;
	uint64_t n_write;
	uint64_t n_dsync;
	uint64_t n_setattr;
	uint64_t n_getattr;
	uint64_t n_execute;
	uint64_t n_replicate;
};

struct anfs_osd_request;
typedef void (*anfs_osd_req_callback_t) (int status, struct anfs_osd_request *r);

struct anfs_osd_request {
	uint64_t id;		/* request id */
	int type;		/* request type AFS_OSD_RQ_... */
	int status;		/* return code after processing */
	int dev;		/* device processed the request */
	uint64_t ino;		/* inode # involved, or collection id */
	uint64_t cid;

	int destdev;		/* for creating a replication */
	struct anfs_task *task;	/* task for execute request */

	void *buf;		/* data in/out buffer, or object list */
	size_t size;		/* data to be transferred */
	off_t off;		/* data offset */

	void *priv;		/* your param to callback */
	anfs_osd_req_callback_t callback; /* callback function */

	struct list_head list;	/* internally-used list */

	uint64_t t_submit;	/* request service time */
	uint64_t t_complete;
};

struct anfs_osd_worker {
	pthread_t id;		/* thread id */
	int dev;		/* the device index i'm working on */
	bool direct;		/* use direct osdlib? */

	pthread_mutex_t lock;	/* lock for accessing the @rq */
	struct list_head rq;	/* request queue */

	struct anfs_osd_dev osd;	/* the osd device handled by this thread */
	struct anfs_osd_rstat stat; /* request statistics */
	uint64_t idle_sleep;	/* idle sleep time in usec */

	/** access service threads from workers */
	struct anfs_osd_worker *copier;
	struct anfs_osd_worker *checker;
};

struct anfs_osd {
	int ndev;
	bool direct;
	struct anfs_osd_worker *workers;	/* dev request handlers */
	struct anfs_osd_worker *copier;	/* copy handlers */
	struct anfs_osd_worker *checker;	/* task status polling */
};

/**
 * anfs_osd_init initializes the osd component. this spawns worker threads for
 * each osd device and establishes connections to devices.
 *
 * @self: anfs_osd structure, the space should be allocated by the caller.
 * @ndev: number of devices. (the # of entries in @devpaths)
 * @devpaths: the device paths.
 * @direct: use direct osdlib (1) or not (0). the direct interface is not
 * currently implemented.
 * @idle_sleep: idle sleep time for worker threads, in usec.
 *
 * returns 0 on success, negatives on errors.
 */
int anfs_osd_init(struct anfs_osd *self, int ndev, char **devpaths, int direct,
		uint64_t idle_sleep);

/**
 * anfs_osd_exit terminates the osd component.
 *
 * @self: anfs_osd structure.
 */
void anfs_osd_exit(struct anfs_osd *self);

/**
 * anfs_osd_submit_request submit an osd request. this function only queues the
 * request to a worker thread who is responsible for the actual processing. the
 * caller should examine the execution status via his own callback function.
 *
 * @self: anfs_osd structure.
 * @req: request encoded using anfs_osd_request.
 *
 * returns 0 on success.
 */
int anfs_osd_submit_request(struct anfs_osd *self, struct anfs_osd_request *req);

/**
 * TODO:
 * The following functions work synchronously.
 */

/**
 * 
 *
 * @self
 * @pid
 * @cid
 *
 * 
 */
int anfs_osd_create_collection(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t *cid);

/**
 * 
 *
 * @self
 * @pid
 * @cid
 * @objs
 * @len
 *
 * 
 */
int anfs_osd_set_membership(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t cid, uint64_t *objs, uint32_t len);

/**
 * 
 *
 * @self
 * @dev
 * @pid
 * @oid
 * @size
 *
 * 
 */
int anfs_osd_get_file_size(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t oid, uint64_t *size);

#endif	/** __AFS_OSD_H__ */

