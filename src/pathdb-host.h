#ifndef	__PATHDB_HOST_H__
#define	__PATHDB_HOST_H__
/**
 * for the fast evaluation, we create a shared database which maps oid to
 * directory path names.
 * targets will reference this database to create a directory path for symbolic
 * links.
 *
 * in the host side, we only need to write the record [object id:ns pathname].
 * in the target side, we only need to read the record.
 *
 * this file is interface for the host.
 */

#include <stdint.h>
#include <sqlite3.h>

struct anfs_pathdb {
	sqlite3 *conn;
	int status;
	sqlite3_stmt **stmts;
	char sqlbuf[2048];
};

int anfs_pathdb_init(struct anfs_pathdb *self, const char *dbfile);

int anfs_pathdb_exit(struct anfs_pathdb *self);

int anfs_pathdb_insert(struct anfs_pathdb *self, uint64_t ino,
			const char *path);

int anfs_pathdb_unlink(struct anfs_pathdb *self, const char *path);

int anfs_pathdb_rename(struct anfs_pathdb *self, const char *old,
			const char *new);

int anfs_pathdb_set_object(struct anfs_pathdb *self, uint64_t ino, int osd,
				uint64_t oid);

#endif	/** __PATHDB_H__ */

