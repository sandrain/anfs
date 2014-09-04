/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * job script parser interface, only used by anfs_sched.
 */
#ifndef	__AFS_PARSER_H__
#define	__AFS_PARSER_H__

struct anfs_job;

int anfs_parser_parse_script(const char *buf, size_t len,
				struct anfs_job **job);

void anfs_parser_cleanup_job(struct anfs_job *job);

#if 0
/**
 * do we need another thread here??
 */
#include <pthread.h>

struct anfs_parser {
	struct anfs_ctx *ctx;
	pthread_t worker;
	pthread_mutex_t wq_lock;
	struct list_head wq;
};

struct anfs_parser_data {
	struct anfs_parser *parser;

	const char *buf;
	size_t len;
	struct anfs_job *job;

	pthread_cond_t cond;	/** async callback via cond_wait/signal */
	pthread_mutex_t lock;

	struct list_head list;
};

int anfs_parser_init(struct anfs_parser *self, struct anfs_ctx *ctx);

void anfs_parser_exit(struct anfs_parser *self);

int anfs_parser_parse_script(struct anfs_parser *self, const char *buf,
				size_t len, struct anfs_job **job);
#endif


#endif	/** __AFS_PARSER_H__ */

