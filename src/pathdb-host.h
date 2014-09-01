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

struct afs_pathdb {
	sqlite3 *conn;
	int status;
	sqlite3_stmt **stmts;
	char sqlbuf[2048];
};

int afs_pathdb_init(struct afs_pathdb *self, const char *dbfile);

int afs_pathdb_exit(struct afs_pathdb *self);

int afs_pathdb_insert(struct afs_pathdb *self, uint64_t pid, uint64_t oid,
			const char *nspath);

int afs_pathdb_remove(struct afs_pathdb *self, uint64_t pid, uint64_t oid);

int afs_pathdb_update(struct afs_pathdb *self, uint64_t pid, uint64_t oid,
			const char *nspath);

#endif	/** __PATHDB_H__ */

