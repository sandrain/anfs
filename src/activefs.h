/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * 
 */
#ifndef	__AFS_ACTIVEFS_H__
#define	__AFS_ACTIVEFS_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#define	AFS_MAX_DEV		32

/**
 * in-memory superblock.
 */
struct afs_super {
	uint64_t id;
	uint64_t magic;
	uint64_t version;
	uint64_t ctime;
	int ndev;
	const char *devs;
	int stripe;
	uint64_t stsize;
	uint64_t stwidth;
	int direct;
	uint64_t root;		/* inode # of root */
	uint64_t i_jobs;	/* inodes # of virtual entries */
	uint64_t i_failed;
	uint64_t i_running;
	uint64_t i_submit;
};

static inline int afs_super_is_virtroot(struct afs_super *self, uint64_t ino)
{
	if (self->i_jobs == ino)
		return 1;
	else if (self->i_failed == ino)
		return 1;
	else if (self->i_running == ino)
		return 1;
	else if (self->i_submit == ino)
		return 1;
	else
		return 0;
}

/**
 * available striping policies.
 */
enum {
	AFS_STRIPE_NONE		= 0,
	AFS_STRIPE_VIRTIO	= 1,
	AFS_STRIPE_STATIC	= 2,
/*	AFS_STRIPE_JOBAWARE	= 3,*/

	AFS_N_STRIPE_MODES,
};

struct afs_stripe {
	struct stat stat;

	int stmode;		/* stripe mode */
	uint64_t stsize;	/* stripe size (chunk size) */
	uint64_t stwidth;	/* stripe width */
	int stloc;		/* file location for lineage mode */

	int dirty;		/* needs attr sync? */
};

enum {
	AFS_JOB_STATUS_WAIT,	/* waiting for execution */
	AFS_JOB_STATUS_RUNNING,	/* running on */
	AFS_JOB_STATUS_COMPLETE,/* successfully completed */
	AFS_JOB_STATUS_ABORT,	/* aborted due to errors */

	AFS_JOB_N_STATUS,
};

struct afs_active_job {
	uint64_t id;		/* job id */
	uint64_t t_submit;	/* timestamp when submit */
	uint64_t t_complete;	/* timestamp when complete */
	const char *script;	/* job script path */
	int status;		/* job status (AFS_JOB_STATUS_...) */
	int error;		/* error code */
};

#include "util.h"
#include "config.h"
#include "db.h"
#include "osd.h"
#include "filer.h"
#include "virtio.h"
#include "lineage.h"
#include "sched.h"
#include "parser.h"

#include "pathdb-host.h"

struct afs_ctx {
	struct afs_super super;

	struct afs_config config;
	struct afs_db db;
	struct afs_lineage lineage;
	struct afs_osd osd;
	struct afs_sched sched;
	struct afs_virtio virtio;

	struct afs_pathdb pathdb;

	char mountpoint[512];
};

#define	afs_super(x)	(&(x)->super)
#define	afs_config(x)	(&(x)->config)
#define	afs_db(x)	(&(x)->db)
#define afs_lineage(x)	(&(x)->lineage)
#define	afs_osd(x)	(&(x)->osd)
#define	afs_sched(x)	(&(x)->sched)
#define afs_virtio(x)	(&(x)->virtio)
#define afs_pathdb(x)	(&(x)->pathdb)

#define afs_ctx(ptr, member)	container_of(ptr, struct afs_ctx, member)

#endif	/** __AFS_ACTIVEFS_H__ */

