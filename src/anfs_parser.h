/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * job script parser interface, only used by anfs_sched.
 */
#ifndef	__ANFS_PARSER_H__
#define	__ANFS_PARSER_H__

struct anfs_job;

int anfs_parser_parse_script(const char *buf, size_t len,
				struct anfs_job **job);

void anfs_parser_cleanup_job(struct anfs_job *job);

#endif	/** __ANFS_PARSER_H__ */

