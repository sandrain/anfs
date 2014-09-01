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
	PATHDB_STMTS
};

static const char *sqls[] = {
	"INSERT INTO afs_nspath (pid, oid, nspath, runtime) VALUES (?,?,?,?)",
	"UPDATE afs_nspath SET nspath=? WHERE pid=? AND oid=?",
	"DELETE FROM afs_nspath WHERE pid=? AND oid=?"
};

static inline sqlite3_stmt *stmt_get(struct afs_pathdb *self, int index)
{
	return self->stmts[index];
}

int afs_pathdb_init(struct afs_pathdb *self, const char *dbfile)
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
	return 0;

out_prepare_sql:
	for (--i ; i >= 0; i--)
		sqlite3_finalize(stmts[i]);
	free(stmts);
out_close:
	sqlite3_close(self->conn);
out:
	return ret;
}

int afs_pathdb_exit(struct afs_pathdb *self)
{
	int ret = 0;

	if (self) {
		if (self->stmts) {
			int i;

			for (i = 0; i < PATHDB_STMTS; i++)
				sqlite3_finalize(self->stmts[i]);

			free(self->stmts);
		}

		if (self->conn)
			ret = sqlite3_close(self->conn);
	}

	return ret;
}

static inline uint64_t get_kernel_runtime(const char *path)
{
	/** montage */
	if (strstr(path, "mImgtbl") != NULL)
		return 1;
	else if (strstr(path, "mProjectPP") != NULL)
		return 5;
	else if (strstr(path, "mAdd") != NULL)
		return 5;
	else if (strstr(path, "mJPEG") != NULL)
		return 6;
	else if (strstr(path, "mOverlaps") != NULL)
		return 1;
	else if (strstr(path, "mDiffFit") != NULL)
		return 2;
	else if (strstr(path, "mBgModel") != NULL)
		return 1;
	else if (strstr(path, "mBgExec") != NULL)
		return 3;
	/** brain */
	else if (strstr(path, "align_warp") != NULL)
		return 3;
	else if (strstr(path, "reslice") != NULL)
		return 4;
	else if (strstr(path, "softmean") != NULL)
		return 13;
	else if (strstr(path, "slicer") != NULL)
		return 4;
	else if (strstr(path, "convert") != NULL)
		return 1;
	else
		return 0;
}

int afs_pathdb_insert(struct afs_pathdb *self, uint64_t pid, uint64_t oid,
			const char *nspath)
{
	int ret;
	uint64_t runtime;
	sqlite3_stmt *stmt;

	runtime = get_kernel_runtime(nspath);

	stmt = stmt_get(self, PATHDB_STMT_INSERT);

	ret = 0;
	ret |= sqlite3_bind_int64(stmt, 1, pid);
	ret |= sqlite3_bind_int64(stmt, 2, oid);
	ret |= sqlite3_bind_text(stmt, 3, nspath, -1, SQLITE_STATIC);
	ret |= sqlite3_bind_int64(stmt, 4, runtime);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

int afs_pathdb_remove(struct afs_pathdb *self, uint64_t pid, uint64_t oid)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, PATHDB_STMT_REMOVE);

	ret = 0;
	ret |= sqlite3_bind_int64(stmt, 1, pid);
	ret |= sqlite3_bind_int64(stmt, 2, oid);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

int afs_pathdb_update(struct afs_pathdb *self, uint64_t pid, uint64_t oid,
			const char *nspath)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, PATHDB_STMT_UPDATE);

	ret = 0;
	ret |= sqlite3_bind_text(stmt, 1, nspath, -1, SQLITE_STATIC);
	ret |= sqlite3_bind_int64(stmt, 2, pid);
	ret |= sqlite3_bind_int64(stmt, 3, oid);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

