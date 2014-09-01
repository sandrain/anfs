/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * job script parser interface, only used by afs_sched.
 */
#ifndef	__AFS_PARSER_H__
#define	__AFS_PARSER_H__

struct afs_job;

int afs_parser_parse_script(const char *buf, size_t len, struct afs_job **job);

void afs_parser_cleanup_job(struct afs_job *job);

#if 0
/**
 * do we need another thread here??
 */
#include <pthread.h>

struct afs_parser {
	struct afs_ctx *ctx;
	pthread_t worker;
	pthread_mutex_t wq_lock;
	struct list_head wq;
};

struct afs_parser_data {
	struct afs_parser *parser;

	const char *buf;
	size_t len;
	struct afs_job *job;

	pthread_cond_t cond;	/** async callback via cond_wait/signal */
	pthread_mutex_t lock;

	struct list_head list;
};

int afs_parser_init(struct afs_parser *self, struct afs_ctx *ctx);

void afs_parser_exit(struct afs_parser *self);

int afs_parser_parse_script(struct afs_parser *self, const char *buf,
				size_t len, struct afs_job **job);
#endif


#endif	/** __AFS_PARSER_H__ */

