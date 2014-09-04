/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * The osd wrapping interface for internal use.
 *
 * hs: changes in anfs (from activefs)
 * - we no more provide the I/O routines, but only provide the workflow
 *   execution related functionalities.
 *
 */
#ifndef	__ANFS_OSD_H__
#define	__ANFS_OSD_H__

#include <unistd.h>
#include <pthread.h>
#include <open-osd/libosd.h>

/**
 * osd request job type.
 */
enum {
	ANFS_OSD_RQ_UNKNOWN	= 0,
	/** XXX: currently, get/setattr are not implemented. */
	ANFS_OSD_RQ_SETATTR,	/* set attribute of an object */
	ANFS_OSD_RQ_GETATTR,	/* get attribute of an object */
	ANFS_OSD_RQ_EXECUTE,	/* submit an active task */
	ANFS_OSD_RQ_CREAT_COLLECTION,	/* create an collection */
	ANFS_OSD_RQ_SET_MEMBERSHIP,	/* associate objects to a collection */
};

/**
 * request statistics.
 *
 * TODO: this should be moved to somewhere else? probably into the in-memory
 * superblock structure.
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
	int type;		/* request type ANFS_OSD_RQ_... */
	int status;		/* return code after processing */
	int dev;		/* device processed the request */
	uint64_t ino;		/* inode # or collection id */
	uint64_t cid;
	uint64_t partition;	/* partition id in osd */

	struct anfs_task *task;	/* task for execute request */

	void *buf;		/* data in/out buffer, or object list */
	size_t size;		/* data to be transferred */

	void *priv;		/* your param to callback */
	anfs_osd_req_callback_t callback; /* callback function */

	struct list_head list;	/* internally-used list */

	uint64_t t_submit;	/* request service time */
	uint64_t t_complete;

	struct osd_dev *osd;	/* set internally */
};

struct anfs_osd_worker {
	pthread_t id;		/* thread id */
	int dev;		/* the device index i'm working on */

	pthread_mutex_t lock;	/* lock for accessing the @rq */
	struct list_head rq;	/* request queue */

	struct osd_dev *osd;	/* the osd device handled by this thread */
	struct anfs_osd_rstat stat; /* request statistics */
	uint64_t idle_sleep;	/* idle sleep time in usec */

	/** access service threads from workers */
	struct anfs_osd_worker *checker;
};

struct anfs_osd {
	uint64_t partition;			/* partition id */
	int ndev;
	char **devpaths;
	struct anfs_osd_worker *workers;	/* dev request handlers */
	struct anfs_osd_worker *checker;	/* task status polling */
};

/**
 * anfs_osd_init initializes the osd component. this spawns worker threads for
 * each osd device and establishes connections to devices.
 *
 * @self: anfs_osd structure, the space should be allocated by the caller.
 * @ndev: number of devices. (the # of entries in @devpaths)
 * @devpaths: the device paths.
 * @idle_sleep: idle sleep time for worker threads, in usec.
 *
 * returns 0 on success, negatives on errors.
 */
int anfs_osd_init(struct anfs_osd *self, int ndev, char **devpaths,
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

int anfs_osd_create_collection(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t *cid);

int anfs_osd_set_membership(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t cid, uint64_t *objs, uint32_t len);

int anfs_osd_get_object_size(struct anfs_osd *self, int dev, uint64_t pid,
				uint64_t oid, uint64_t *size);

#endif	/** __ANFS_OSD_H__ */

