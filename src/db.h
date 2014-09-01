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
 * postgres, or bdb) should be considered.
 */
#ifndef	__AFS_DB_H__
#define	__AFS_DB_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <sqlite3.h>

struct afs_db {
	sqlite3 *conn;		/* sqlite3 handle */
	int status;		/* status of the final request */
	sqlite3_stmt **stmts;	/* pre-compiled statements */
	char sqlbuf[2048];	/* temporal sql buffer */
};

/**
 * all db access functions (afs_db_...) do not initiate transactions. if you
 * need transactional operations, you have to wrap your transactions between
 * afs_tx_begin() and afs_tx_commit(). (afs_tx_rollback() on errors)
 */

/**
 * afs_db_init initializes the database context.
 *
 * @self: afs_db, which will be initialized by this function.
 * @dbfile: the path to the database file.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_db_init(struct afs_db *self, const char *dbfile);

/**
 * afs_db_exit terminates the db connection.
 *
 * @self: afs_db context.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_db_exit(struct afs_db *self);

/**
 * 
 *
 * @self
 *
 * 
 */
int afs_db_tx_begin(struct afs_db *self);

/**
 * 
 *
 * @self
 *
 * 
 */
int afs_db_tx_commit(struct afs_db *self);

/**
 * 
 *
 * @self
 *
 * 
 */
int afs_db_tx_rollback(struct afs_db *self);

/**
 * afs_db_read_super reads superblock from the database.
 *
 * @self: afs_db.
 * @super: the superblock structure which will be filled by this function.
 * @version: the version # to read. this feature is not implemented, yet. the
 * lastest one is always fetched.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_db_read_super(struct afs_db *self, struct afs_super *super,
			const int version);

/**
 * afs_db_lookup_entry lookups the specified entry and fills up the output
 * params if found.
 *
 * @self: afs_db context.
 * @parent: parent inode.
 * @name: entry name.
 * @attr: [out] struct stat to be set.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_db_lookup_entry(struct afs_db *self, const uint64_t parent,
			const char *name, struct stat *attr);

/**
 * 
 *
 * @self
 * @ino
 * @attr
 *
 * returns 0 (normal) or 1 (virtual) on success. negatives on errors.
 */
int afs_db_getattr(struct afs_db *self, const uint64_t ino, struct stat *attr);

/**
 * 
 *
 * @self
 * @ino
 * @attr
 *
 * 
 */
int afs_db_setattr(struct afs_db *self, const uint64_t ino, struct stat *attr);

typedef	int (*afs_db_dirent_callback_t) (void *, int, char **, char **);

/**
 * 
 *
 * @self
 * @ino
 * @callback
 * @param
 *
 * 
 */
int afs_db_dirent_iter(struct afs_db *self, const uint64_t ino,
			afs_db_dirent_callback_t callback, void *param);

/**
 * 
 *
 * @self
 * @parent
 * @mode
 * @rdev
 * @attr
 * @virt
 *
 * 
 */
int afs_db_inode_append(struct afs_db *self, const uint64_t parent, mode_t mode,
			dev_t rdev, struct stat *attr, int virt);

/**
 *
 *
 * @self
 * @parent
 * @ino
 * @name
 * @isdir: if specified, '.' and '..' entries are appended as well.
 * @virt
 *
 */
int afs_db_dirent_append(struct afs_db *self, const uint64_t parent,
			const uint64_t ino, const char *name, int isdir,
			int virt);

/**
 * 
 *
 * @self
 * @parent
 * @name
 * @ino: [out] removed inode number will be set.
 * @keep: specify whether the removed entry will be kept in the unlinked table.
 * (0: nope, other values: keep it)
 *
 * 
 */
int afs_db_dirent_remove(struct afs_db *self, const uint64_t parent,
			const char *name, int64_t *ino, int keep);

/**
 * 
 *
 * @self
 * @ino
 * @sinfo
 *
 * 
 */
int afs_db_get_stripe_info(struct afs_db *self, const uint64_t ino,
			struct afs_stripe *sinfo);

/**
 * 
 *
 * @self
 * @ino
 * @size
 *
 * 
 */
int afs_db_update_inode_size(struct afs_db *self, const uint64_t ino,
			uint64_t size);

/**
 * afs_db_unlink decreases the link count of the specified inode. if the link
 * count reaches zero, pre-registered sql trigger moves the directory entries
 * belonging to the inode to the unlinked entry table.
 *
 * @self: afs_db context.
 * @parent: parent directory inode.
 * @name: name for the directory entry.
 *
 * returns 0 on success, negatives on errors.
 */
int afs_db_unlink(struct afs_db *self, const uint64_t parent, const char *name);

/**
 * 
 *
 * @self
 * @ino
 * @link
 *
 * 
 */
int afs_db_create_symlink(struct afs_db *self, const uint64_t ino,
			const char *link);

/**
 * 
 *
 * @self
 * @ino
 * @link: [out] the pointer to string contains the link path. XXX: who frees
 * this?
 *
 * 
 */
int afs_db_read_symlink(struct afs_db *self, const uint64_t ino,
			const char **link);

/**
 * 
 *
 * @self
 *
 * 
 */
int afs_db_remove_virtual_entries(struct afs_db *self);

/**
 * 
 *
 * @self
 * @ino
 *
 * 
 */
int afs_db_is_virtual_inode(struct afs_db *self, const uint64_t ino);

/**
 * 
 *
 * @self
 * @path
 * @ino
 *
 * 
 */
int afs_db_find_inode_from_path(struct afs_db *self, const char *path,
				struct stat *stbuf);

/**
 * 
 *
 * @self
 * @ino
 * @dev
 *
 * 
 */
int afs_db_add_replication(struct afs_db *self, uint64_t ino, int dev);

/**
 * 
 *
 * @self
 * @ino
 * @dev
 *
 * 
 */
int afs_db_replication_available(struct afs_db *self, uint64_t ino, int dev);

/**
 * 
 *
 * @self
 * @n
 * @cids
 *
 * 
 */
int afs_db_assign_collection_ids(struct afs_db *self, int n, uint64_t *cids);

#if 0
/**
 * 
 *
 * @self
 * @ino
 * @size
 *
 * 
 */
int afs_db_update_file_size(struct afs_db *self, uint64_t ino, uint64_t size);
#endif

/**
 * 
 *
 * @self
 * @ino
 * @stloc
 * @size
 *
 * 
 */
int afs_db_update_task_output_file(struct afs_db *self, uint64_t ino,
					int stloc, uint64_t size);

/**
 * 
 *
 * @self
 * @ino
 *
 * 
 */
int afs_db_invalidate_replica(struct afs_db *self, uint64_t ino);

/**
 * 
 *
 * @self
 * @ino
 * @pathbuf: the caller should allocate the buffer where the path will be
 * copied in
 *
 * 
 */
int afs_db_get_full_path(struct afs_db *self, uint64_t ino, char *pathbuf);

#endif	/** __AFS_DB_H__ */

