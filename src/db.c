/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * metadata db implementation.
 */
#include <sys/time.h>
#include "activefs.h"

enum {
	AFS_DB_STMT_GETSUPER	= 0,	/* 00 */
	AFS_DB_STMT_SETSUPER,		/* 01 */
	AFS_DB_STMT_LOOKUP,		/* 02 */
	AFS_DB_STMT_GETATTR,		/* 03 */
	AFS_DB_STMT_SETATTR,		/* 04 */
	AFS_DB_STMT_MKNOD,		/* 05 */
	AFS_DB_STMT_NEWDIRENT,		/* 06 */
	AFS_DB_STMT_DELDIRENT,		/* 07 */
	AFS_DB_STMT_GETSINFO,		/* 08 */
	AFS_DB_STMT_UNLINK,		/* 09 */
	AFS_DB_STMT_MVUNLINKED,		/* 10 */
	AFS_DB_STMT_SYMLINK,		/* 11 */
	AFS_DB_STMT_READLINK,		/* 12 */
	AFS_DB_STMT_ISVIRT,		/* 13 */
	AFS_DB_STMT_ADDREPLICA,		/* 14 */
	AFS_DB_STMT_SEARCHREPLICA,	/* 15 */
	AFS_DB_STMT_UPDATETASKOUTPUT,	/* 16 */
	AFS_DB_STMT_INVALREPLICA,	/* 17 */
	AFS_DB_STMT_FINDPATH,		/* 18 */

	AFS_DB_N_STMTS,
};

static const char *sqls[] = {
	/** 00.GETSUPER */
	"SELECT * FROM afs_super ORDER BY id DESC LIMIT 1",

	/** 01.SETSUPER */
	"UPDATE afs_super SET version=?,ctime=?,root=? WHERE id=?",

	/** 02.LOOKUP */
	"SELECT id,mode,nlink,uid,gid,rdev,size,ctime,atime,mtime "
	"FROM afs_inode WHERE id=("
	"SELECT e_ino FROM afs_dirent WHERE d_ino=? AND name=?)",

	/** 03.GETATTR */
	"SELECT id,mode,nlink,uid,gid,rdev,size,ctime,atime,mtime,virtual "
	"FROM afs_inode WHERE id=?",

	/** 04.SETATTR */
	"UPDATE afs_inode "
	"SET mode=?,nlink=?,uid=?,gid=?,rdev=?,size=?,ctime=?,atime=?,mtime=? "
	"WHERE id=?",

	/** 05.MKNOD */
	"INSERT INTO afs_inode "
	"(mode,nlink,uid,gid,rdev,size,ctime,atime,mtime,stmode,virtual) "
	"VALUES (?,?,?,?,?,?,?,?,?,?,?)",

	/** 06.NEWDIRENT */
	"INSERT INTO afs_dirent (d_ino,e_ino,virtual,name) VALUES (?,?,?,?)",

	/** 07.DELDIRENT */
	"DELETE FROM afs_dirent WHERE d_ino=? AND name=?",

	/** 08.GETSINFO */
	"SELECT * FROM afs_inode WHERE id=? ",

	/** 09.UNLINK */
	"UPDATE afs_inode SET nlink=nlink-1 WHERE id=("
	"SELECT e_ino FROM afs_dirent WHERE d_ino=? AND name=?)",

	/** 10.MVUNLINKED */
	"INSERT INTO afs_dirent_unlinked (d_ino,e_ino,name) VALUES (?,?,?)",

	/** 11.SYMLINK */
	"INSERT INTO afs_symlink (ino,path) VALUES (?,?)",

	/** 12.READLINK */
	"SELECT path FROM afs_symlink WHERE ino=?",

	/** 13.ISVIRT */
	"SELECT id FROM afs_inode WHERE id=? AND virtual=1",

	/** 14.ADDREPLICA */
	"INSERT INTO afs_replica (ino,dev) VALUES (?,?)",

	/** 15.SEARCHREPLICA */
	"SELECT id FROM afs_replica WHERE ino=? AND dev=?",

	/** 16.UPDATETASKOUTPUT */
	"UPDATE afs_inode SET size=?,stloc=? WHERE id=?",

	/** 17. INVALREPLICA */
	"DELETE FROM afs_replica WHERE ino=?",

	/** 18. FINDPATH */
	"SELECT d_ino,name FROM afs_dirent WHERE e_ino=? LIMIT 1",
};

static inline sqlite3_stmt *stmt_get(struct afs_db *self, int index)
{
	return self->stmts[index];
}

static inline int db_exec_simple_sql(struct afs_db *self, const char *sql)
{
	int ret;
	ret = sqlite3_exec(self->conn, sql, NULL, NULL, NULL);
	return ret == SQLITE_OK ? 0 : ret;
}

static inline int begin_transaction(struct afs_db *self)
{
	return db_exec_simple_sql(self, "BEGIN TRANSACTION;");
}

static inline int end_transaction(struct afs_db *self)
{
	return db_exec_simple_sql(self, "END TRANSACTION;");
}

static inline int rollback_transaction(struct afs_db *self)
{
	return db_exec_simple_sql(self, "ROLLBACK;");
}

/**
 * XXX: Be warned that set temp_store as a memory space can cause some problems!
 */
static inline int set_pragma(struct afs_db *self)
{
	return db_exec_simple_sql(self, "PRAGMA synchronous = OFF; "
					"PRAGMA journal_mode = MEMORY; "
					"PRAGMA temp_store = 2;");
}

static inline int set_pragma_safe(struct afs_db *self)
{
	return db_exec_simple_sql(self, "PRAGMA synchronous = OFF; ");
}

static inline void fill_stat_attr(sqlite3_stmt *stmt, struct stat *attr)
{
	attr->st_ino = sqlite3_column_int64(stmt, 0);
	attr->st_mode = sqlite3_column_int(stmt, 1);
	attr->st_nlink = sqlite3_column_int(stmt, 2);
	attr->st_uid = sqlite3_column_int(stmt, 3);
	attr->st_gid = sqlite3_column_int(stmt, 4);
	attr->st_rdev = sqlite3_column_int(stmt, 5);
	attr->st_size = sqlite3_column_int(stmt, 6);
	attr->st_ctime = sqlite3_column_int(stmt, 7);
	attr->st_atime = sqlite3_column_int(stmt, 8);
	attr->st_mtime = sqlite3_column_int(stmt, 9);
}

int afs_db_init(struct afs_db *self, const char *dbfile)
{
	int i, ret;
	char *buf;
	sqlite3_stmt **stmts = NULL;

	if (!self)
		return -EINVAL;

	ret = sqlite3_open(dbfile, &self->conn);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	stmts = calloc(sizeof(*stmts), AFS_DB_N_STMTS);
	if (stmts == NULL) {
		ret = -ENOMEM;
		goto out_close;
	}

#if 0
	set_pragma_safe(self);
#endif
	set_pragma(self);

	for (i = 0; i < AFS_DB_N_STMTS; i++) {
		ret = sqlite3_prepare(self->conn, sqls[i], -1,
					&stmts[i], NULL);
		if (ret != SQLITE_OK)
			goto out_prepare_sql;
	}

	self->stmts = stmts;

	return 0;

out_prepare_sql:
	for (--i ; i >= 0; i--)
		sqlite3_finalize(stmts[i]);
out_free:
	free(stmts);
out_close:
	sqlite3_close(self->conn);
out:
	return ret;
}

int afs_db_exit(struct afs_db *self)
{
	int ret = 0;

	if (self) {
		if (self->stmts) {
			int i;

			for (i = 0; i < AFS_DB_N_STMTS; i++)
				sqlite3_finalize(self->stmts[i]);

			free(self->stmts);
		}

		if (self->conn)
			ret = sqlite3_close(self->conn);
	}

	return ret;
}

int afs_db_tx_begin(struct afs_db *self)
{
	return begin_transaction(self);
}

int afs_db_tx_commit(struct afs_db *self)
{
	return end_transaction(self);
}

int afs_db_tx_rollback(struct afs_db *self)
{
	return rollback_transaction(self);
}

int afs_db_read_super(struct afs_db *self, struct afs_super *super,
			const int version)
{
	int ret;
	sqlite3_stmt *stmt;

	__afs_unused(version);	/** not implemented, yet */

	if (!self || !super)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_GETSUPER);

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
	super->stsize = sqlite3_column_int64(stmt, 6);
	super->stwidth = sqlite3_column_int64(stmt, 7);
	super->direct = sqlite3_column_int(stmt, 8);
	super->root = sqlite3_column_int64(stmt, 9);
	super->i_jobs = sqlite3_column_int64(stmt, 10);
	super->i_failed = sqlite3_column_int64(stmt, 11);
	super->i_running = sqlite3_column_int64(stmt, 12);
	super->i_submit = sqlite3_column_int64(stmt, 13);

	ret = 0;
out:
	/** do not need calling sqlite3_reset */

	return ret;
}

int afs_db_lookup_entry(struct afs_db *self, const uint64_t parent,
			const char *name, struct stat *attr)
{
	int ret;
	int nlink;
	sqlite3_stmt *stmt;

	if (!self || !attr)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_LOOKUP);
	ret = sqlite3_bind_int64(stmt, 1, parent);
	ret = sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = -EIO;
		goto out;
	}

	fill_stat_attr(stmt, attr);
	ret = 0;
out:
	sqlite3_reset(stmt);

	return ret;
}

int afs_db_getattr(struct afs_db *self, const uint64_t ino, struct stat *attr)
{
	int ret;
	sqlite3_stmt *stmt;

	if (!self || !attr)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_GETATTR);
	ret = sqlite3_bind_int64(stmt, 1, ino);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = -EIO;
		goto out;
	}

	fill_stat_attr(stmt, attr);
	ret = sqlite3_column_int(stmt, 10); /** 0 for normal, 1 for vitual */
out:
	sqlite3_reset(stmt);

	return ret;
}

int afs_db_setattr(struct afs_db *self, const uint64_t ino, struct stat *attr)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self || !attr)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_SETATTR);

	ret = sqlite3_bind_int(stmt, 1, attr->st_mode);
	ret |= sqlite3_bind_int(stmt, 2, attr->st_nlink);
	ret |= sqlite3_bind_int(stmt, 3, attr->st_uid);
	ret |= sqlite3_bind_int(stmt, 4, attr->st_gid);
	ret |= sqlite3_bind_int(stmt, 5, attr->st_rdev);
	ret |= sqlite3_bind_int64(stmt, 6, attr->st_size);
	ret |= sqlite3_bind_int64(stmt, 7, attr->st_ctime);
	ret |= sqlite3_bind_int64(stmt, 8, attr->st_atime);
	ret |= sqlite3_bind_int64(stmt, 9, attr->st_mtime);
	ret |= sqlite3_bind_int64(stmt, 10, ino);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	ret = ret == SQLITE_DONE ? 0 : -EIO;

	sqlite3_reset(stmt);
	return ret;
}

int afs_db_dirent_iter(struct afs_db *self, const uint64_t ino,
			afs_db_dirent_callback_t callback, void *param)
{
	char sql[512];

	if (!self || !callback)
		return -EINVAL;

	sprintf(sql, "SELECT e_ino, name FROM afs_dirent WHERE d_ino = %llu"
		     ,afs_llu(ino));

	return sqlite3_exec(self->conn, sql, callback, param, NULL);
}

int afs_db_inode_append(struct afs_db *self, const uint64_t parent,
			mode_t mode, dev_t rdev, struct stat *attr, int virt)
{
	int ret;
	uint64_t now;
	sqlite3_stmt *stmt;
	struct stat pstat;	/** parent stat */

	if (!self || !attr)
		return -EINVAL;

	ret = afs_db_getattr(self, parent, &pstat);
	if (ret < 0)
		return -EIO;

	now = afs_now();
	stmt = stmt_get(self, AFS_DB_STMT_MKNOD);

	ret = 0;
	ret |= sqlite3_bind_int(stmt, 1, mode);
	ret |= sqlite3_bind_int(stmt, 2, 1);
	ret |= sqlite3_bind_int(stmt, 3, pstat.st_uid);
	ret |= sqlite3_bind_int(stmt, 4, pstat.st_gid);
	ret |= sqlite3_bind_int(stmt, 5, rdev);
	ret |= sqlite3_bind_int64(stmt, 6, attr->st_size);
	ret |= sqlite3_bind_int64(stmt, 7, now);
	ret |= sqlite3_bind_int64(stmt, 8, now);
	ret |= sqlite3_bind_int64(stmt, 9, now);
	ret |= sqlite3_bind_int(stmt, 10, virt ? AFS_STRIPE_VIRTIO : 0);
	ret |= sqlite3_bind_int(stmt, 11, virt ? 1 : 0);
	if (ret) {
		ret = -EIO;
		goto out_err;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret == SQLITE_DONE) {
		ret = 0;

		attr->st_ino = sqlite3_last_insert_rowid(self->conn);
		attr->st_dev = 1;
		attr->st_mode = mode;
		attr->st_nlink = 1;
		attr->st_uid = pstat.st_uid;
		attr->st_gid = pstat.st_gid;
		attr->st_rdev = rdev;
		/*attr->st_size = 0;*/
		attr->st_ctime = now;
		attr->st_atime = now;
		attr->st_mtime = now;
	}
	else
		ret = -EIO;

out_err:
	sqlite3_reset(stmt);

	return ret;
}

int afs_db_dirent_append(struct afs_db *self, const uint64_t parent,
			const uint64_t ino, const char *name, int isdir,
			int virt)
{
	int ret = 0;
	sqlite3_stmt *stmt;
	struct stat stbuf;

	if (!self || !name)
		return -EINVAL;

	ret = afs_db_getattr(self, parent, &stbuf);
	if (ret < 0)
		return -EIO;

	stmt = stmt_get(self, AFS_DB_STMT_NEWDIRENT);

	ret = sqlite3_bind_int64(stmt, 1, parent);
	ret |= sqlite3_bind_int64(stmt, 2, ino);
	ret |= sqlite3_bind_int64(stmt, 3, virt ? 1 : 0);
	ret |= sqlite3_bind_text(stmt, 4, name, -1, SQLITE_STATIC);
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

	if (!isdir) {
		ret = 0;
		goto out;
	}

	/**
	 * create . and .. for directories.
	 */

	sqlite3_reset(stmt);
	ret = 0;
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_int64(stmt, 2, ino);
	ret |= sqlite3_bind_int(stmt, 3, 0);
	ret |= sqlite3_bind_text(stmt, 4, ".", -1, SQLITE_STATIC);

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_DONE) {
		ret = -EIO;
		goto out;
	}

	sqlite3_reset(stmt);
	ret = 0;
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_int64(stmt, 2, parent);
	ret |= sqlite3_bind_int(stmt, 3, 0);
	ret |= sqlite3_bind_text(stmt, 4, "..", -1, SQLITE_STATIC);

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

int afs_db_dirent_remove(struct afs_db *self, const uint64_t parent,
			const char *name, int64_t *ino, int keep)
{
	int ret = 0;
	struct stat stbuf;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	ret = afs_db_lookup_entry(self, parent, name, &stbuf);
	if (ret)
		return -EIO;

	stmt = stmt_get(self, AFS_DB_STMT_DELDIRENT);
	ret = sqlite3_bind_int64(stmt, 1, parent);
	ret = sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
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

	if (keep) {
		sqlite3_reset(stmt);

		stmt = stmt_get(self, AFS_DB_STMT_MVUNLINKED);
		ret = 0;
		ret |= sqlite3_bind_int64(stmt, 1, parent);
		ret |= sqlite3_bind_int64(stmt, 2, stbuf.st_ino);
		ret |= sqlite3_bind_text(stmt, 3, name, -1, SQLITE_STATIC);
		if (ret) {
			ret = -EIO;
			goto out;
		}

		do {
			ret = sqlite3_step(stmt);
		} while (ret == SQLITE_BUSY);

		ret = ret == SQLITE_DONE ? 0 : -EIO;
	}
	else
		ret = 0;

	*ino = stbuf.st_ino;
out:
	sqlite3_reset(stmt);
	return ret;
}

int afs_db_get_stripe_info(struct afs_db *self, const uint64_t ino,
			struct afs_stripe *sinfo)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self || !sinfo)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_GETSINFO);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret != SQLITE_ROW) {
		ret = -EIO;
		goto out;
	}

	fill_stat_attr(stmt, &sinfo->stat);

	sinfo->stmode = sqlite3_column_int(stmt, 10);
	sinfo->stsize = sqlite3_column_int64(stmt, 11);
	sinfo->stwidth = sqlite3_column_int64(stmt, 12);
	sinfo->stloc = sqlite3_column_int(stmt, 13);

	ret = 0;
out:
	sqlite3_reset(stmt);
	return ret;
}

int afs_db_unlink(struct afs_db *self, const uint64_t parent, const char *name)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_UNLINK);
	ret = sqlite3_bind_int64(stmt, 1, parent);
	ret |= sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
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

int afs_db_create_symlink(struct afs_db *self, const uint64_t ino,
			const char *link)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_SYMLINK);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	ret |= sqlite3_bind_text(stmt, 2, link, -1, SQLITE_STATIC);
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

int afs_db_read_symlink(struct afs_db *self, const uint64_t ino,
			const char **link)
{
	int ret;
	sqlite3_stmt *stmt;
	const char *tmp;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_READLINK);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret == SQLITE_DONE) {
		ret = -ENOENT;
		goto out;
	}

	if (ret != SQLITE_ROW) {
		ret = -EIO;
		goto out;
	}

	tmp = (const char *) sqlite3_column_text(stmt, 0);
	*link = strdup(tmp);
	ret = 0;
out:
	sqlite3_reset(stmt);
	return ret;
}

int afs_db_remove_virtual_entries(struct afs_db *self)
{
	int ret;

	do {
		ret = sqlite3_exec(self->conn,
				"delete from afs_inode where virtual=1; "
				"delete from afs_dirent where virtual=1; ",
				NULL, NULL, NULL);
	} while (ret == SQLITE_BUSY);

	return ret == SQLITE_OK ? 0 : -EIO;
}

int afs_db_is_virtual_inode(struct afs_db *self, const uint64_t ino)
{
	int ret;
	sqlite3_stmt *stmt;

	stmt = stmt_get(self, AFS_DB_STMT_ISVIRT);
	ret = sqlite3_bind_int64(stmt, 1, ino);
	if (ret) {
		ret = -EIO;
		goto out;
	}

	do {
		ret = sqlite3_step(stmt);
	} while (ret == SQLITE_BUSY);

	if (ret == SQLITE_ROW)
		ret = 1;
	else if (ret == SQLITE_DONE)
		ret = 0;
	else
		ret = -EIO;

out:
	sqlite3_reset(stmt);
	return ret;
}

int afs_db_find_inode_from_path(struct afs_db *self, const char *path,
				struct stat *stbuf)
{
	int ret;
	uint64_t parent;
	char *sp, *tmp;
	char *buf = afs_strdup(path);
	struct stat statbuf;

	parent = afs_super(afs_ctx(self, db))->root;

	while ((sp = strsep(&buf, "/")) != NULL) {
		if (strempty(sp))
			continue;

		ret = afs_db_lookup_entry(self, parent, sp, &statbuf);
		if (ret)
			return ret;

		parent = statbuf.st_ino;
	}

	*stbuf = statbuf;
	return 0;
}

int afs_db_add_replication(struct afs_db *self, uint64_t ino, int dev)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_ADDREPLICA);
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

int afs_db_replication_available(struct afs_db *self, uint64_t ino, int dev)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_SEARCHREPLICA);
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

int afs_db_assign_collection_ids(struct afs_db *self, int n, uint64_t *cids)
{
	int i, ret = 0;
	sqlite3_stmt *stmt;

	if (!self || !cids)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_MKNOD);

	for (i = 0; i < n; i++) {
		ret = 0;
		ret |= sqlite3_bind_int(stmt, 1, 0);
		ret |= sqlite3_bind_int(stmt, 2, 0);
		ret |= sqlite3_bind_int(stmt, 3, 0);
		ret |= sqlite3_bind_int(stmt, 4, 0);
		ret |= sqlite3_bind_int(stmt, 5, 0);
		ret |= sqlite3_bind_int64(stmt, 6, 0);
		ret |= sqlite3_bind_int64(stmt, 7, 0);
		ret |= sqlite3_bind_int64(stmt, 8, 0);
		ret |= sqlite3_bind_int64(stmt, 9, 0);
		ret |= sqlite3_bind_int(stmt, 10, 0);
		ret |= sqlite3_bind_int(stmt, 11, 0);

		if (ret)
			goto out_err;

		do {
			ret = sqlite3_step(stmt);
		} while (ret == SQLITE_BUSY);

		if (ret != SQLITE_DONE)
			goto out_err;

		cids[i] = sqlite3_last_insert_rowid(self->conn);
		sqlite3_reset(stmt);
	}

	return 0;

out_err:
	ret = -EIO;
	sqlite3_reset(stmt);
	return ret;
}

#if 0
int afs_db_update_file_size(struct afs_db *self, uint64_t ino, uint64_t size)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_SETFILESIZE);
	ret = sqlite3_bind_int64(stmt, 1, size);
	ret != sqlite3_bind_int64(stmt, 2, ino);
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
#endif

int afs_db_update_task_output_file(struct afs_db *self, uint64_t ino,
					int stloc, uint64_t size)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_UPDATETASKOUTPUT);
	ret = sqlite3_bind_int64(stmt, 1, size);
	ret = sqlite3_bind_int(stmt, 2, stloc);
	ret != sqlite3_bind_int64(stmt, 3, ino);
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

int afs_db_invalidate_replica(struct afs_db *self, uint64_t ino)
{
	int ret = 0;
	sqlite3_stmt *stmt;

	if (!self)
		return -EINVAL;

	stmt = stmt_get(self, AFS_DB_STMT_INVALREPLICA);
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

int afs_db_get_full_path(struct afs_db *self, uint64_t ino, char *pathbuf)
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

	stmt = stmt_get(self, AFS_DB_STMT_FINDPATH);

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

