/* Copyright (C) 2013 Hyogi Sim <hyogi@cs.vt.edu>
 *
 *
 * ---------------------------------------------------------------------------
 * Refer to COPYING for the license of this program.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sqlite3.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "anfs.h"

enum {
	MDB_SQL_GETSUPER	= 0,
	MDB_SQL_FINDINO,	/* 1 */
	MDB_SQL_INODEAPPEND,
	MDB_SQL_DIRENTAPPEND,
	MDB_SQL_DIRENTREMOVE,
	MDB_SQL_SYMLINKAPPEND,	/* 5 */
	MDB_SQL_GETATTR,
	MDB_SQL_READLINK,
	MDB_SQL_CHMOD,
	MDB_SQL_CHOWN,
	MDB_SQL_TRUNCATE,	/* 10 */
	MDB_SQL_UTIME,
	MDB_SQL_READDIR,
	MDB_SQL_GETLOCATION,
	MDB_SQL_FINDPATH,
	MDB_SQL_TASKOUTPUT,	/* 15 */
	MDB_SQL_ADDREPLICA,
	MDB_SQL_SEARCHREPLICA,
	MDB_SQL_INVALREPLICA,

	N_MDB_SQLS
};

static const char *mdb_sqls[N_MDB_SQLS] = {
	/** 00. MDB_SQL_GETSUPER */
	"select * from anfs_super order by id desc limit 1",

	/** 01. MDB_SQL_FINDINO */
	"select e_ino from anfs_dirent where d_ino=? and name=?",

	/** 02. MDB_SQL_INODEAPPEND */
	"insert into anfs_inode "
	"(dev,mode,nlink,uid,gid,rdev,size,atime,mtime,ctime) values "
	"(?,?,?,?,?,?,?,?,?,?)",

	/** 03. MDB_SQL_DIRENTAPPEND */
	"insert into anfs_dirent (d_ino,e_ino,name) values (?,?,?)",

	/** 04. MDB_SQL_DIRENTREMOVE */
	"delete from anfs_dirent where d_ino=? and name=?",

	/** 05. MDB_SQL_SYMLINKAPPEND */
	"insert into anfs_symlink (ino,path) values (?,?)",

	/** 06. MDB_SQL_GETATTR */
	"select * from anfs_inode where id=?",

	/** 07. MDB_SQL_READLINK */
	"select path from anfs_symlink where ino=?",

	/** 08. MDB_SQL_CHMOD */
	"update anfs_inode set mode=? where id=?",

	/** 09. MDB_SQL_CHOWN */
	"update anfs_inode set uid=?,gid=? where id=?",

	/** 10. MDB_SQL_TRUNCATE */
	"update anfs_inode set size=? where id=?",

	/** 11. MDB_SQL_UTIME */
	"update anfs_inode set atime=?,mtime=? where id=?",

	/** 12. MDB_SQL_READDIR */
	/** limit <skip>,<count> */
	"select e_ino,name from anfs_dirent where d_ino=? limit ?,?",

	/** 13. MDB_SQL_GETLOCATION */
	"select stloc from anfs_inode where id=?",

	/** 14. MDB_SQL_FINDPATH */
	"select d_ino,name from anfs_dirent where e_ino=? limit 1",

	/** 15. MDB_SQL_TASKOUTPUT */
	"update anfs_inode set size=?,stloc=? where id=?",

	/** 16. MDB_SQL_ADDREPLICA */
	"insert into anfs_replica (ino,dev) values (?,?)",

	/** 17. MDB_SQL_SEARCHREPLICA */
	"select id from anfs_replica where ino=? and dev=?",

	/** 18. MDB_SQL_INVALREPLICA */
	"delete from afs_replica where ino=?",
};

/**
 * internal helper functions
 */
static inline int exec_simple_sql(struct anfs_mdb *self,
				const char *sql)
{
	int ret;
	ret = sqlite3_exec(self->conn, sql, NULL, NULL, NULL);
	return ret == SQLITE_OK ? 0 : ret;
}

static inline int set_pragma(struct anfs_mdb *self, int safe)
{
	if (safe)
		return exec_simple_sql(self, "PRAGMA synchronous = OFF;");
	else
		return exec_simple_sql(self, "PRAGMA synchronous = OFF; "
				      "PRAGMA temp_store = 2;");
}

static inline sqlite3_stmt *stmt_get(struct anfs_mdb *self, int id)
{
	return self->stmts[id];
}

static inline int validate_mode(mode_t mode)
{
	int ret = 0;
	unsigned int ifmt = mode & S_IFMT;

	if (S_ISCHR(mode) || S_ISBLK(mode)
		|| S_ISFIFO(mode) || S_ISSOCK(mode)) {
		return EINVAL;
	}

	if (S_ISREG(mode)) {
		if (ifmt & ~S_IFREG)
			ret = EINVAL;
	}
	else if (S_ISDIR(mode)) {
		if (ifmt & ~S_IFDIR)
			ret = EINVAL;
	}

	return ret;
}

static int lookup_dirent(struct anfs_mdb *self, uint64_t parent,
			const char *name, uint64_t *ino_out)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_FINDINO);
	ret = sqlite3_bind_int64(stmt, 1, parent);
	ret |= sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = ret == SQLITE_DONE ? -ENOENT : -EIO;
		goto out;
	}

	*ino_out = sqlite3_column_int64(stmt, 0);
	ret = 0;
out:
	sqlite3_reset(stmt);
	return ret;
}

static int find_ino_from_path(struct anfs_mdb *self,
				const char *path, uint64_t *ino_out)
{
	int ret;
	uint64_t ino, parent;
	char *sp, *tmp;
	char *buf = strdup(path);
	size_t len = strlen(buf);

	if (len > 1 && buf[len - 1] == '/')
		buf[len - 1] = '\0';

	parent = 1;
	while ((sp = strsep(&buf, "/")) != NULL) {
		if (strempty(sp))
			continue;

		ret = lookup_dirent(self, parent, sp, &ino);
		if (ret)
			goto out;

		parent = ino;
	}

	*ino_out = parent;
	ret = 0;
out:
	free(buf);
	return ret;
}

static inline int is_root_path(const char *path)
{
	const char *pos = path;

	while (*pos)
		if (*pos++ != '/')
			return 0;

	return 1;
}

/**
 * TODO; check if path can contain consecutive '/'s.
 * e.g. "///" for root
 */

/**
 *the memory space should be deallocated by the caller.
 */
static inline char *parent_path(const char *path)
{
	char *pos, *buf;

	if (is_root_path(path))
		return (char *) path;

	buf = strdup(path);
	pos = strrchr(buf, '/');
	pos[0] = '\0';

	return buf;
}

static inline const char *filename(const char *path)
{
	const char *pos;

	if (is_root_path(path))
		return path;

	pos = strrchr(path, '/');
	return &pos[1];
}

static int find_parent_ino_from_path(struct anfs_mdb *self,
				const char *path, uint64_t *ino_out)
{
	int ret;
	char *parent = parent_path(path);

	ret = find_ino_from_path(self, parent, ino_out);
	free(parent);

	return ret;
}

static int inode_append(struct anfs_mdb *self, struct stat *stbuf,
			uint64_t *ino_out)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_INODEAPPEND);
	ret = sqlite3_bind_int(stmt, 1, stbuf->st_dev);
	ret |= sqlite3_bind_int(stmt, 2, stbuf->st_mode);
	ret |= sqlite3_bind_int(stmt, 3, stbuf->st_nlink);
	ret |= sqlite3_bind_int(stmt, 4, stbuf->st_uid);
	ret |= sqlite3_bind_int(stmt, 5, stbuf->st_gid);
	ret |= sqlite3_bind_int(stmt, 6, stbuf->st_rdev);
	ret |= sqlite3_bind_int64(stmt, 7, stbuf->st_size);
	ret |= sqlite3_bind_int64(stmt, 8, stbuf->st_atime);
	ret |= sqlite3_bind_int64(stmt, 9, stbuf->st_mtime);
	ret |= sqlite3_bind_int64(stmt, 10, stbuf->st_ctime);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret == SQLITE_DONE) {
		*ino_out = sqlite3_last_insert_rowid(self->conn);
		ret = 0;
	}
	else
		ret = -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

static int dirent_append(struct anfs_mdb *self, uint64_t pino,
			uint64_t ino, const char *name, int isdir)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_DIRENTAPPEND);
	ret = sqlite3_bind_int64(stmt, 1, pino);
	ret |= sqlite3_bind_int64(stmt, 2, ino);
	ret |= sqlite3_bind_text(stmt, 3, name, -1, SQLITE_STATIC);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_DONE) {
		ret = -EIO;
		goto out;
	}

	ret = 0;
	if (!isdir)
		goto out;

	/**
	 * create . and .. entries.
	 */
	sqlite3_reset(stmt);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_int64(stmt, 2, ino);
	ret |= sqlite3_bind_text(stmt, 3, ".", -1, SQLITE_STATIC);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_DONE) {
		ret = -EIO;
		goto out;
	}

	ret = 0;
	sqlite3_reset(stmt);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_int64(stmt, 2, pino);
	ret |= sqlite3_bind_text(stmt, 3, "..", -1, SQLITE_STATIC);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_DONE) {
		ret = -EIO;
		goto out;
	}

	ret = 0;
out:
	sqlite3_reset(stmt);
	return ret;
}

static int dirent_remove(struct anfs_mdb *self, const uint64_t pino,
			const char *name)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_DIRENTREMOVE);
	ret = sqlite3_bind_int64(stmt, 1, pino);
	ret |= sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_DONE) {
		ret = -EIO;
		goto out;
	}

	if (sqlite3_changes(self->conn) == 0) {
		ret = -ENOENT;
		goto out;
	}

	ret = 0;
out:
	sqlite3_reset(stmt);
	return ret;
}

static int symlink_append(struct anfs_mdb *self, uint64_t ino,
			const char *path)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_SYMLINKAPPEND);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_text(stmt, 2, path, -1, SQLITE_STATIC);
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

static int get_inode_attr(struct anfs_mdb *self, uint64_t ino,
			struct stat *stbuf)
{
	int ret, index;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_GETATTR);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = ret == SQLITE_DONE ? -ENOENT : -EIO;
		goto out;
	}

	stbuf->st_ino = ino;
	stbuf->st_dev = sqlite3_column_int(stmt, 1);
	stbuf->st_mode = sqlite3_column_int(stmt, 2);
	stbuf->st_nlink = sqlite3_column_int(stmt, 3);
	stbuf->st_uid = sqlite3_column_int(stmt, 4);
	stbuf->st_gid = sqlite3_column_int(stmt, 5);
	stbuf->st_rdev = sqlite3_column_int(stmt, 6);
	stbuf->st_size = sqlite3_column_int64(stmt, 7);
	stbuf->st_blocks = (stbuf->st_size >> 12) + 1;
	stbuf->st_atime = sqlite3_column_int64(stmt, 8);
	stbuf->st_ctime = sqlite3_column_int64(stmt, 9);
	stbuf->st_mtime = sqlite3_column_int64(stmt, 10);
	/** dirty hack: blksize is always 4096, we use this space for storing
	 * the file location.
	 */
	index = sqlite3_column_int(stmt, 11);
	stbuf->st_blksize = index < 0 ? 4096 : index;

	ret = 0;
out:
	sqlite3_reset(stmt);
	return ret;
}

/**
 * external APIs
 */

int anfs_mdb_tx_begin(struct anfs_mdb *self)
{
	return exec_simple_sql(self, "BEGIN TRANSACTION");
}

int anfs_mdb_tx_commit(struct anfs_mdb *self)
{
	return exec_simple_sql(self, "END TRANSACTION");
}

int anfs_mdb_tx_abort(struct anfs_mdb *self)
{
	return exec_simple_sql(self, "ROLLBACK");
}

#define	anfs_mdb_tx_rollback(s)		anfs_mdb_tx_abort(s)

int anfs_mdb_get_ino(struct anfs_mdb *self, const char *path, uint64_t *ino)
{
	return find_ino_from_path(self, path, ino);
}

int anfs_mdb_getattr(struct anfs_mdb *self, const char *path,
			struct stat *buf)
{
	int ret;
	uint64_t ino;

	ret = find_ino_from_path(self, path, &ino);
	if (ret)
		return ret;

	return get_inode_attr(self, ino, buf);
}

int anfs_mdb_get_ino_loc(struct anfs_mdb *self, const char *path,
			uint64_t *ino, int *loc)
{
	struct stat buf;
	struct anfs_ctx *ctx = anfs_ctx(self, mdb);
	int ret = anfs_mdb_getattr(self, path, &buf);

	if (ret)
		return ret;

	*ino = buf.st_ino;
	*loc = buf.st_blksize == 4096 ?
			*ino % anfs_super(ctx)->ndev : buf.st_blksize;

	return 0;
}

int anfs_mdb_readlink(struct anfs_mdb *self, const char *path, char *link,
			size_t size)
{
	int ret;
	uint64_t ino;
	sqlite3_stmt *stmt;
	const char *tmp;

	ret = find_ino_from_path(self, path, &ino);
	if (ret)
		return ret;

	stmt = stmt_get(self, MDB_SQL_READLINK);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = ret == SQLITE_DONE ? -ENOENT : -EIO;
		goto out;
	}

	tmp = (const char *) sqlite3_column_text(stmt, 0);
	if (strlen(tmp) <= size)
		strcpy(link, tmp);
	else {
		strncpy(link, tmp, size);
		link[size-1] = '\0';
	}
	ret = 0;

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_mknod(struct anfs_mdb *self, const char *path, mode_t mode,
			dev_t dev, uint64_t *ino_out)
{
	int ret;
	uint64_t ino, pino;
	struct stat statbuf;

	ret = validate_mode(mode);
	if (ret) {
		ret = -EINVAL;
		goto out_err;
	}

	ret = find_parent_ino_from_path(self, path, &pino);
	if (ret)
		goto out_err;
	ret = get_inode_attr(self, pino, &statbuf);
	if (ret)
		goto out_err;

	statbuf.st_dev = dev;
	statbuf.st_mode = mode;
	statbuf.st_nlink = 1;
	statbuf.st_atime = statbuf.st_mtime = statbuf.st_ctime
			= anfs_now_sec();

	if (S_ISDIR(mode))
		statbuf.st_size = 4096;
	else if (S_ISLNK(mode))
		statbuf.st_size = *ino_out;
	else
		statbuf.st_size = 0;

	anfs_mdb_tx_begin(self);

	ret = inode_append(self, &statbuf, &ino);
	if (ret)
		goto out_rollback;

	ret = dirent_append(self, pino, ino, filename(path), S_ISDIR(mode));
	if (ret)
		goto out_rollback;

	anfs_mdb_tx_commit(self);
	*ino_out = ino;
	return 0;

out_err:
	return ret;
out_rollback:
	anfs_mdb_tx_abort(self);
	return -EIO;
}

int anfs_mdb_mkdir(struct anfs_mdb *self, const char *path, mode_t mode)
{
	uint64_t ino;

	mode &= ALLPERMS;
	mode |= S_IFDIR;

	return anfs_mdb_mknod(self, path, mode, 0, &ino);
}

int anfs_mdb_unlink(struct anfs_mdb *self, const char *path)
{
	int ret;
	uint64_t pino;

	ret = find_parent_ino_from_path(self, path, &pino);
	if (ret)
		return ret;

	return dirent_remove(self, pino, filename(path));
}

int anfs_mdb_rmdir(struct anfs_mdb *self, const char *path)
{
	return anfs_mdb_unlink(self, path);
}

int anfs_mdb_symlink(struct anfs_mdb *self, const char *path, const char *link)
{
	int ret;
	uint64_t sino, dino, pino; /** source/dest and dest parent ino */

	ret = find_parent_ino_from_path(self, link, &pino);
	if (ret)
		return ret;

	anfs_mdb_tx_begin(self);

	/** dirty hack: pass the size info throught the output ino pointer */
	dino = strlen(path);
	ret = anfs_mdb_mknod(self, link,
			S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO, 0, &dino);
	if (ret)
		goto out_rollback;

	ret = symlink_append(self, dino, path);
	if (ret)
		goto out_rollback;

	anfs_mdb_tx_commit(self);
	return 0;

out_rollback:
	anfs_mdb_tx_abort(self);
	return ret;
}

int anfs_mdb_rename(struct anfs_mdb *self, const char *old, const char *new)
{
	int ret;
	uint64_t old_pino, new_pino, ino;

	ret = find_parent_ino_from_path(self, old, &old_pino);
	if (ret)
		return ret;
	ret = find_parent_ino_from_path(self, new, &new_pino);
	if (ret)
		return ret;
	ret = find_ino_from_path(self, old, &ino);
	if (ret)
		return ret;

	anfs_mdb_tx_begin(self);

	ret = anfs_mdb_unlink(self, old);
	if (ret)
		goto out_rollback;

	ret = dirent_append(self, new_pino, ino, filename(new), 0);
	if (ret)
		goto out_rollback;

	anfs_mdb_tx_commit(self);
	return 0;

out_rollback:
	anfs_mdb_tx_abort(self);
	return ret;
}

int anfs_mdb_chmod(struct anfs_mdb *self, const char *path, mode_t mode)
{
	int ret;
	uint64_t ino;
	sqlite3_stmt *stmt;

	ret = find_ino_from_path(self, path, &ino);
	if (ret)
		return ret;

	stmt = stmt_get(self, MDB_SQL_CHMOD);
	ret = sqlite3_bind_int(stmt, 1, mode);
	ret |= sqlite3_bind_int64(stmt, 2, ino);
	if (ret)
		goto out;

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_chown(struct anfs_mdb *self, const char *path, uid_t uid,
			gid_t gid)
{
	int ret;
	uint64_t ino;
	sqlite3_stmt *stmt;

	ret = find_ino_from_path(self, path, &ino);
	if (ret)
		return ret;

	stmt = stmt_get(self, MDB_SQL_CHOWN);
	ret = sqlite3_bind_int(stmt, 1, uid);
	ret |= sqlite3_bind_int(stmt, 2, gid);
	ret |= sqlite3_bind_int64(stmt, 3, ino);
	if (ret)
		goto out;

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_truncate(struct anfs_mdb *self, const char *path, off_t newsize)
{
	int ret;
	uint64_t ino;
	sqlite3_stmt *stmt;

	ret = find_ino_from_path(self, path, &ino);
	if (ret)
		return ret;

	stmt = stmt_get(self, MDB_SQL_TRUNCATE);
	ret = sqlite3_bind_int64(stmt, 1, newsize);
	ret |= sqlite3_bind_int64(stmt, 2, ino);
	if (ret)
		goto out;

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_utime(struct anfs_mdb *self, const char *path,
			struct utimbuf *tbuf)
{
	int ret;
	uint64_t ino;
	sqlite3_stmt *stmt;

	ret = find_ino_from_path(self, path, &ino);
	if (ret)
		return ret;

	stmt = stmt_get(self, MDB_SQL_UTIME);
	ret = sqlite3_bind_int64(stmt, 1, tbuf->actime);
	ret |= sqlite3_bind_int64(stmt, 2, tbuf->modtime);
	ret |= sqlite3_bind_int64(stmt, 3, ino);
	if (ret)
		goto out;

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_readdir(struct anfs_mdb *self, const char *path, uint64_t pos,
			struct anfs_dirent *dirent)
{
	int ret;
	uint64_t dino, ino;
	const char *tmp;
	sqlite3_stmt *stmt;

	ret = find_ino_from_path(self, path, &dino);
	if (ret)
		return ret;

	stmt = stmt_get(self, MDB_SQL_READDIR);
	ret = sqlite3_bind_int64(stmt, 1, dino);
	ret |= sqlite3_bind_int64(stmt, 2, pos);
	ret |= sqlite3_bind_int(stmt, 3, 1);
	if (ret)
		goto out;

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = ret == SQLITE_DONE ? 0 : -EIO;
		goto out;
	}

	dirent->d_ino = sqlite3_column_int64(stmt, 0);
	tmp = (const char *) sqlite3_column_text(stmt, 1);
	if (strlen(tmp) <= ANFS_NAMELEN)
		strcpy(dirent->d_name, tmp);
	else {
		strncpy(dirent->d_name, tmp, ANFS_NAMELEN);
		dirent->d_name[ANFS_NAMELEN-1] = '\0';
	}
	ret = 1;

out:
	sqlite3_reset(stmt);
	return ret;
}

/**
 * note that the resersed path will be overwritten.
 */
static char *reverse_path(char *reversed)
{
	char rpath[1024];
	char path[1024];
	char *pos, *rpos;
#if 0
	char *rpath = calloc(1, strlen(reversed) + 1);
	char *path = strdup(reversed);

	if (!rpath || !path)
		return NULL;	/** ENOMEM */
#endif

	strcpy(path, reversed);
	rpos = rpath;

	pos = strrchr(path, '/');
	*pos = '\0';
	*rpos++ = '/';

	while (pos > path) {
		pos = strrchr(path, '/');
		if (!pos)
			break;
		*pos = '\0';
		rpos += sprintf(rpos, "%s/", ++pos);
	}

	rpos += sprintf(rpos, "%s%c", path, '\0');
	//free(path);

	if (strlen(reversed) != strlen(rpath)) {
		errno = EIO;
		return NULL;
	}

	/** copy the path to the original memory space */
	strcpy(reversed, rpath);
	//free(rpath);

	return reversed;
}

int anfs_mdb_get_full_path(struct anfs_mdb *self, uint64_t ino, char *pathbuf)
{
	int ret = 0;
	uint64_t cino = ino;
	char *pos = pathbuf;
	sqlite3_stmt *stmt;

	if (!self || !pathbuf)
		return -EINVAL;

	/** for the root inode, no need to bother the db */
	if (cino == 1) {
		pathbuf[0] = '/';
		pathbuf[1] = '\0';
		return ret;
	}

	stmt = stmt_get(self, MDB_SQL_FINDPATH);

	while (1) {
		ret = sqlite3_bind_int64(stmt, 1, cino);
		if (ret) {
			ret = -EIO;
			goto out;
		}

		do {
			ret = sqlite3_step(stmt);
		} while (ret == SQLITE_BUSY);

		if (ret != SQLITE_ROW)
			break;

		cino = sqlite3_column_int(stmt, 0);
		pos += sprintf(pos, "%s/", sqlite3_column_text(stmt, 1));

		sqlite3_reset(stmt);

		if (cino == 1)	/* reached the root */
			break;
	}

	pos = reverse_path(pathbuf);
	ret = 0;

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_update_task_output_file(struct anfs_mdb *self, uint64_t ino,
					int location, uint64_t size)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_TASKOUTPUT);
	ret = sqlite3_bind_int64(stmt, 1, size);
	ret |= sqlite3_bind_int64(stmt, 2, location);
	ret |= sqlite3_bind_int64(stmt, 3, ino);
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

int anfs_mdb_add_replication(struct anfs_mdb *self, uint64_t ino, int dev)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, MDB_SQL_ADDREPLICA);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_int(stmt, 2, dev);

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

int anfs_mdb_replication_available(struct anfs_mdb *self, uint64_t ino,
					int dev)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, MDB_SQL_SEARCHREPLICA);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_int(stmt, 2, dev);

	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_ROW ? 1 : 0;

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_invalidate_replica(struct anfs_mdb *self, uint64_t ino)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, MDB_SQL_INVALREPLICA);
	ret = sqlite3_bind_int64(stmt, 1, ino);
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

int anfs_mdb_get_file_location(struct anfs_mdb *self, uint64_t ino,
				int *location)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, MDB_SQL_GETLOCATION);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = ret == SQLITE_DONE ? -ENOENT : -EIO;
		goto out;
	}

	*location = sqlite3_column_int64(stmt, 0);

out:
	sqlite3_reset(stmt);
	return ret;
}

int anfs_mdb_init(struct anfs_mdb *self, const char *dbfile)
{
	int i, ret = 0;
	sqlite3_stmt **stmts = NULL;

	if (!self)
		return -EINVAL;

	ret = sqlite3_open(dbfile, &self->conn);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	stmts = calloc(sizeof(*stmts), N_MDB_SQLS);
	if (stmts == NULL) {
		ret = -ENOMEM;
		goto out_close;
	}

	set_pragma(self, 0);

	for (i = 0; i < N_MDB_SQLS; i++) {
		ret = sqlite3_prepare(self->conn, mdb_sqls[i], -1,
					&stmts[i], NULL);
		if (ret != SQLITE_OK)
			goto out_prepare_sql;
	}

	self->stmts = stmts;

	return 0;

out_prepare_sql:
	for (--i; i >= 0; i--)
		sqlite3_finalize(stmts[i]);
out_free:
	free(stmts);
out_close:
	sqlite3_close(self->conn);
out:
	return ret;
}

int anfs_mdb_exit(struct anfs_mdb *self)
{
	int i, ret = 0;

	if (self) {
		if (self->stmts) {
			for (i = 0; i < N_MDB_SQLS; i++)
				sqlite3_finalize(self->stmts[i]);
			free(self->stmts);
		}

		if (self->conn)
			ret = sqlite3_close(self->conn);
	}

	return ret;
}

int anfs_mdb_read_super(struct anfs_mdb *self, struct anfs_super *super,
			int version)
{
	int ret;
	sqlite3_stmt *stmt;

	__anfs_unused(version);	/** not implemented, yet */

	if (!self || !super)
		return -EINVAL;

	stmt = stmt_get(self, MDB_SQL_GETSUPER);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = -EIO;
		goto out;
	}

	super->id = sqlite3_column_int64(stmt, 0);
	super->magic = sqlite3_column_int64(stmt, 1);
	super->version = sqlite3_column_int64(stmt, 2);
	super->ctime = sqlite3_column_int64(stmt, 3);
	super->ndev = sqlite3_column_int(stmt, 4);
	super->devs = (char *) sqlite3_column_text(stmt, 5);
	super->root = 1;
	super->i_submit = 2;

	ret = 0;
out:
	/** do not need calling sqlite3_reset */

	return ret;
}

