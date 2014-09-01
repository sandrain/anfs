/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * filesystem configurations.
 */
#ifndef	__AFS_CONFIG_H__
#define	__AFS_CONFIG_H__

struct afs_config {
	char *configfile;
	char *dbfile;

	int update_mtime;	/* update timestamp for each ops? */
	int update_atime;

	int sched_policy;	/* job scheduling policy */

	uint64_t worker_idle_sleep;

	char *pathdb_path;	/* pathdb path */
};

/**
 * afs_config_init reads configurations from the specified file.
 *
 * @self: the structure to be initialized by calling this. the space should be
 * allocated by the caller.
 * @cfile: the configuration file.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_config_init(struct afs_config *self, const char *cfile);

/**
 * afs_config_exit deallocates memory space of afs_config
 *
 * @self: afs_config structure.
 */
static inline void afs_config_exit(struct afs_config *self)
{
	if (self) {
		if (self->configfile)
			free(self->configfile);
		if (self->dbfile)
			free(self->dbfile);
	}
}

#endif	/** __AFS_CONFIG_H__ */

