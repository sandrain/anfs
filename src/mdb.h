/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * internal interface of db manager for activefs.
 *
 * XXX; using sqlite3 limits us to serialize all accesses to the database. it
 * is ok for functionality (because, currently, activefs doesn't support
 * multi-threading), though it can hurts the performance.  when performance
 * problem becomes significant (which is very likely), more advanced db (mysql,
 * postgres, or bdb?) should be considered.
 */
#ifndef	__ANFS_MDB_H__
#define	__ANFS_MDB_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>
#include <errno.h>
#include <sqlite3.h>

struct anfs_mdb {
	sqlite3 *conn;
	int status;
	sqlite3_stmt **stmts;
	char sqlbuf[2048];
};

#define	ANFS_NAMELEN	256

struct anfs_dirent {
	uint64_t d_ino;
	char d_name[ANFS_NAMELEN];
	void *d_priv;
	struct stat stat;
};

/** TODO: put some documentation here. */

/**
 * anfs filesystem functions
 */

int anfs_mdb_init(struct anfs_mdb *self, const char *dbfile);

int anfs_mdb_exit(struct anfs_mdb *self);

int anfs_mdb_read_super(struct anfs_mdb *self, struct anfs_super *super,
			int version);

int anfs_mdb_tx_begin(struct anfs_mdb *self);

int anfs_mdb_tx_commit(struct anfs_mdb *self);

int anfs_mdb_tx_abort(struct anfs_mdb *self);

int anfs_mdb_get_ino(struct anfs_mdb *self, const char *path, uint64_t *ino);

int anfs_mdb_get_ino_loc(struct anfs_mdb *self, const char *path,
			uint64_t *ino, int *loc);

int anfs_mdb_getattr(struct anfs_mdb *self, const char *path,
			struct stat *buf);

int anfs_mdb_readlink(struct anfs_mdb *self, const char *path, char *link,
			size_t size);

int anfs_mdb_mknod(struct anfs_mdb *self, const char *path, mode_t mode,
			dev_t dev, uint64_t *ino_out);

int anfs_mdb_mkdir(struct anfs_mdb *self, const char *path, mode_t mode);

int anfs_mdb_unlink(struct anfs_mdb *self, const char *path);

int anfs_mdb_rmdir(struct anfs_mdb *self, const char *path);

int anfs_mdb_symlink(struct anfs_mdb *self, const char *path,
			const char *link);

int anfs_mdb_rename(struct anfs_mdb *self, const char *old, const char *new);

int anfs_mdb_chmod(struct anfs_mdb *self, const char *path, mode_t mode);

int anfs_mdb_chown(struct anfs_mdb *self, const char *path, uid_t uid,
			gid_t gid);

int anfs_mdb_truncate(struct anfs_mdb *self, const char *path, off_t newsize);

int anfs_mdb_utime(struct anfs_mdb *self, const char *path,
			struct utimbuf *tbuf);

int anfs_mdb_readdir(struct anfs_mdb *self, const char *path, uint64_t pos,
			struct anfs_dirent *dirent);

/**
 * sched workflow related functions
 */

int anfs_mdb_get_full_path(struct anfs_mdb *self, uint64_t ino, char *path);

int anfs_mdb_update_task_output_file(struct anfs_mdb *self, uint64_t ino,
					int index, uint64_t size);

int anfs_mdb_assign_collection_ids(struct anfs_mdb *self, int n,
					uint64_t *cids);

int anfs_mdb_add_replication(struct anfs_mdb *self, uint64_t ino, int dev);

int anfs_mdb_replication_available(struct anfs_mdb *self, uint64_t ino,
					int dev);

int anfs_mdb_invalidate_replica(struct anfs_mdb *self, uint64_t ino);

int anfs_mdb_get_file_location(struct anfs_mdb *self, uint64_t ino,
				int *index);

#endif

