/**
 * pathdb-host.c
 *
 * All the things are hardcoded.
 */
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "pathdb-host.h"

enum {
	PATHDB_STMT_INSERT	= 0,
	PATHDB_STMT_UPDATE,
	PATHDB_STMT_REMOVE,
	PATHDB_STMT_OBJECT,
	PATHDB_STMTS
};

static const char *sqls[] = {
	"insert into anfs_nspath (ino,nspath) values (?,?)",
	"update anfs_nspath set nspath=? where nspath=?",
	"delete from anfs_nspath where nspath=?",
	"insert into anfs_oids (ino,osd,oid) values (?,?,?)",
};

static inline sqlite3_stmt *stmt_get(struct anfs_pathdb *self, int index)
{
	return self->stmts[index];
}

static inline int exec_simple_sql(struct anfs_pathdb *self,
				const char *sql)
{
	int ret;
	ret = sqlite3_exec(self->conn, sql, NULL, NULL, NULL);
	return ret == SQLITE_OK ? 0 : ret;
}

static inline int begin_transaction(struct anfs_pathdb *self)
{
	return exec_simple_sql(self, "begin transaction");
}

static inline int commit_transaction(struct anfs_pathdb *self)
{
	return exec_simple_sql(self, "end transaction");
}

static inline int abort_transaction(struct anfs_pathdb *self)
{
	return exec_simple_sql(self, "rollback");
}

int anfs_pathdb_init(struct anfs_pathdb *self, const char *dbfile)
{
	int i, ret;
	sqlite3_stmt **stmts = NULL;

	if (!self)
		return -EINVAL;

	ret = sqlite3_open(dbfile, &self->conn);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	stmts = calloc(sizeof(*stmts), PATHDB_STMTS);
	if (stmts == NULL) {
		ret = -ENOMEM;
		goto out_close;
	}

	for (i = 0; i < PATHDB_STMTS; i++) {
		ret = sqlite3_prepare(self->conn, sqls[i], -1,
					&stmts[i], NULL);
		if (ret != SQLITE_OK) {
			fputs(sqlite3_errmsg(self->conn), stderr);
			goto out_prepare_sql;
		}
	}

	self->stmts = stmts;

	return sqlite3_enable_shared_cache(1);

out_prepare_sql:
	for (--i ; i >= 0; i--)
		sqlite3_finalize(stmts[i]);
	free(stmts);
out_close:
	sqlite3_close(self->conn);
out:
	return ret;
}

int anfs_pathdb_exit(struct anfs_pathdb *self)
{
	int i, ret = 0;

	if (self) {
		if (self->stmts) {
			for (i = 0; i < PATHDB_STMTS; i++)
				sqlite3_finalize(self->stmts[i]);
			free(self->stmts);
		}

		if (self->conn)
			ret = sqlite3_close(self->conn);
	}

	return ret;
}

int anfs_pathdb_insert(struct anfs_pathdb *self, uint64_t ino,
			const char *nspath)
{
	int ret;
	uint64_t runtime;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, PATHDB_STMT_INSERT);

	ret = 0;
	ret |= sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_text(stmt, 2, nspath, -1, SQLITE_STATIC);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	begin_transaction(self);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;
	if (ret)
		abort_transaction(self);
	else
		commit_transaction(self);

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_pathdb_unlink(struct anfs_pathdb *self, const char *nspath)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, PATHDB_STMT_REMOVE);

	ret = 0;
	ret |= sqlite3_bind_text(stmt, 1, nspath, -1, SQLITE_STATIC);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	begin_transaction(self);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;
	if (ret)
		abort_transaction(self);
	else
		commit_transaction(self);

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_pathdb_rename(struct anfs_pathdb *self, const char *old,
			const char *new)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, PATHDB_STMT_UPDATE);

	ret = 0;
	ret |= sqlite3_bind_text(stmt, 1, new, -1, SQLITE_STATIC);
	ret |= sqlite3_bind_text(stmt, 2, old, -1, SQLITE_STATIC);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	begin_transaction(self);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;
	if (ret)
		abort_transaction(self);
	else
		commit_transaction(self);

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_pathdb_set_object(struct anfs_pathdb *self, uint64_t ino, int osd,
				uint64_t oid)
{
	int ret;
	uint64_t runtime;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, PATHDB_STMT_OBJECT);

	ret = 0;
	ret |= sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_int(stmt, 2, osd);
	ret |= sqlite3_bind_int64(stmt, 3, oid);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	begin_transaction(self);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret == SQLITE_DONE) {
		ret = 0;
		commit_transaction(self);
	}
	else {
		abort_transaction(self);
		if ((ret = sqlite3_extended_errcode(self->conn))
				== SQLITE_IOERR_BLOCKED)
			ret = 0;	/** this doesn't harm */
		else
			ret = -EIO;
	}

out:
	sqlite3_reset(stmt);
	return 0;
}

