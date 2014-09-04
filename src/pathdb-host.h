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

int anfs_pathdb_insert(struct anfs_pathdb *self, uint64_t pid, uint64_t oid,
			const char *nspath);

int anfs_pathdb_remove(struct anfs_pathdb *self, uint64_t pid, uint64_t oid);

int anfs_pathdb_update(struct anfs_pathdb *self, uint64_t pid, uint64_t oid,
			const char *nspath);

#endif	/** __PATHDB_H__ */

