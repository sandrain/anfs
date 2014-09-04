/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * 
 */
#ifndef	__ANFS_H__
#define	__ANFS_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#define	ANFS_MAX_DEV		32
#define	ANFS_MAGIC		0x414e4653	/** ANFS */

/**
 * in-memory superblock.
 */
struct anfs_super {
	uint64_t id;
	uint64_t magic;
	uint64_t version;
	uint64_t ctime;
	int ndev;
	int direct;		/* obsolete */
	const char *devs;
	uint64_t root;		/* inode # of root */
	uint64_t i_submit;	/* inodes # of virtual entries */
};

enum {
	AFS_JOB_STATUS_WAIT,	/* waiting for execution */
	AFS_JOB_STATUS_RUNNING,	/* running on */
	AFS_JOB_STATUS_COMPLETE,/* successfully completed */
	AFS_JOB_STATUS_ABORT,	/* aborted due to errors */

	AFS_JOB_N_STATUS,
};

struct anfs_active_job {
	uint64_t id;		/* job id */
	uint64_t t_submit;	/* timestamp when submit */
	uint64_t t_complete;	/* timestamp when complete */
	const char *script;	/* job script path */
	int status;		/* job status (AFS_JOB_STATUS_...) */
	int error;		/* error code */
};

#include "util.h"
#include "config.h"
#include "mdb.h"
#include "osd.h"
#include "lineage.h"
//#include "sched.h"
#include "store.h"
#include "parser.h"

#include "pathdb-host.h"

struct anfs_ctx {
	struct anfs_super super;

	struct anfs_config config;
	struct anfs_mdb mdb;
	struct anfs_lineage lineage;
#if 0
	struct anfs_osd osd;
	struct anfs_sched sched;
#endif
	struct anfs_store store;

	struct anfs_pathdb pathdb;

	const char *root;

	char mountpoint[512];	/* obsolete */
};

#define	anfs_super(x)	(&(x)->super)
#define	anfs_config(x)	(&(x)->config)
#define	anfs_mdb(x)	(&(x)->mdb)
#define anfs_lineage(x)	(&(x)->lineage)
#if 0
#define	anfs_osd(x)	(&(x)->osd)
#define	anfs_sched(x)	(&(x)->sched)
#endif
#define anfs_store(x)	(&(x)->store)
#define anfs_pathdb(x)	(&(x)->pathdb)

#define anfs_ctx(ptr, member)	container_of(ptr, struct anfs_ctx, member)

#endif	/** __ANFS_H__ */

