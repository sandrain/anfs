/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * fuse implementation of activefs using low-level fuse API.
 */
#define	FUSE_USE_VERSION	26

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <syslog.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <fuse/fuse_lowlevel.h>

#include "activefs.h"

static struct afs_ctx _ctx;

static int afs_mode_validate(mode_t mode)
{
	int ret = 0;
	unsigned int ifmt = mode & S_IFMT;

	if (S_ISREG(mode)) {
		if (ifmt & ~S_IFREG)
			ret = EINVAL;
	}
	else if (S_ISDIR(mode)) {
		if (ifmt & ~S_IFDIR)
			ret = EINVAL;
	}
	else if (S_ISCHR(mode)) {
		if (ifmt & ~S_IFCHR)
			ret = EINVAL;
	}
	else if (S_ISBLK(mode)) {
		if (ifmt & ~S_IFBLK)
			ret = EINVAL;
	}
	else if (S_ISFIFO(mode)) {
		if (ifmt & ~S_IFIFO)
			ret = EINVAL;
	}
	else if (S_ISLNK(mode)) {
		ret = EINVAL;
	}
	else if (S_ISSOCK(mode)) {
		if (ifmt & ~S_IFSOCK)
			return EINVAL;
	}
	else
		ret = EINVAL;

	return ret;
}

/**
 * readdir helpers, taken from the example (hello_ll.c).
 */

struct dirbuf {
	char *p;
	size_t size;
};

struct dirbuf_iter {
	struct dirbuf buf;
	fuse_req_t req;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf *b, const char *name,
			fuse_ino_t ino)
{
	struct stat stbuf;
	size_t oldsize = b->size;
	b->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
	b->p = (char *) realloc(b->p, b->size);
	memset(&stbuf, 0, sizeof(stbuf));
	stbuf.st_ino = ino;
	fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name,
			&stbuf, b->size);
}

static int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
				off_t off, size_t maxsize)
{
	if (off < bufsize)
		return fuse_reply_buf(req, buf + off,
				afs_min(bufsize - off, maxsize));
	else
		return fuse_reply_buf(req, NULL, 0);
}

/**
 * pathdb handling helpers
 */

static const uint64_t AFS_OBJECT_OFFSET = 0x10000;

static inline int pathdb_insert(struct afs_ctx *ctx, uint64_t ino)
{
	char path[2048];	/* should be enough for now */
	int ret = afs_db_get_full_path(afs_db(ctx), ino, path);

	return afs_pathdb_insert(afs_pathdb(ctx),
			(uint64_t) 0x22222, ino + AFS_OBJECT_OFFSET,
			path);
}

static inline int pathdb_update(struct afs_ctx *ctx, uint64_t ino)
{
	char path[2048];
	int ret = afs_db_get_full_path(afs_db(ctx), ino, path);

	return afs_pathdb_update(afs_pathdb(ctx),
			(uint64_t) 0x22222, ino + AFS_OBJECT_OFFSET,
			path);
}

static inline int pathdb_remove(struct afs_ctx *ctx, uint64_t ino)
{
	return afs_pathdb_remove(afs_pathdb(ctx),
			(uint64_t) 0x22222, ino + AFS_OBJECT_OFFSET);
}

/**
 * row structure: ino, name 
 */
static int afs_dirent_callback(void *param, int argc, char **argv, char **col)
{
	struct dirbuf_iter *di = (struct dirbuf_iter *) param;
	struct dirbuf *buf = &di->buf;
	uint64_t ino = atoll(argv[0]);
	char *name = strdup(argv[1]);

	dirbuf_add(di->req, buf, name, ino);
	free(name);

	return 0;
}

static inline int afs_remove_virtual_entries(struct afs_ctx *ctx)
{
	return afs_db_remove_virtual_entries(afs_db(ctx));
}

/**
 * activefs fuse operations.
 */

static void afs_fuse_lookup(fuse_req_t req, fuse_ino_t parent,
			const char *name)
{
	int ret;
	struct fuse_entry_param entry;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	memset(&entry, 0, sizeof(entry));

	ret = afs_db_lookup_entry(afs_db(ctx), parent, name, &entry.attr);
	if (ret)
		fuse_reply_err(req, ENOENT);
	else if (entry.attr.st_nlink == 0)	/* the entry removed */
		fuse_reply_err(req, ENOENT);
	else {
		entry.ino = entry.attr.st_ino;
		fuse_reply_entry(req, &entry);
	}
}

static void afs_fuse_forget(fuse_req_t req, fuse_ino_t ino,
			unsigned long lookup)
{
	fuse_reply_none(req);
}

static void afs_fuse_getattr(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
{
	int ret;
	struct stat stbuf;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	(void) fi;

	memset(&stbuf, 0, sizeof(stbuf));

	ret = afs_db_getattr(afs_db(ctx), ino, &stbuf);
	if (ret)
		fuse_reply_err(req, ENOENT);
	else
		fuse_reply_attr(req, &stbuf, 1.0);
}

static void afs_fuse_setattr(fuse_req_t req, fuse_ino_t ino,
			struct stat *attr, int to_set,
			struct fuse_file_info *fi)
{
	int ret;
	int dirty = 0;
	struct stat current;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	ret = afs_db_getattr(afs_db(ctx), ino, &current);
	if (ret)
		goto eio;

	if (to_set & FUSE_SET_ATTR_MODE) {
		current.st_mode = attr->st_mode;
		dirty = 1;
	}
	if (to_set & FUSE_SET_ATTR_UID) {
		current.st_uid = attr->st_uid;
		dirty = 1;
	}
	if (to_set & FUSE_SET_ATTR_GID) {
		current.st_gid = attr->st_gid;
		dirty = 1;
	}
	if (to_set & FUSE_SET_ATTR_SIZE) {
		current.st_size = attr->st_size;
		dirty = 1;
	}
	if (to_set & FUSE_SET_ATTR_ATIME) {
		current.st_atime = attr->st_atime;
		dirty = 1;
	}
	if (to_set & FUSE_SET_ATTR_MTIME) {
		current.st_mtime = attr->st_mtime;
		dirty = 1;
	}

	if (dirty) {
		ret = afs_db_setattr(afs_db(ctx), ino, &current);
		if (ret)
			goto eio;
	}

	fuse_reply_attr(req, &current, 2.0);
	return;
eio:
	fuse_reply_err(req, EIO);
}

static void afs_fuse_readlink(fuse_req_t req, fuse_ino_t ino)
{
	int ret;
	const char *link = NULL;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	ret = afs_db_read_symlink(afs_db(ctx), ino, &link);
	if (ret) {
		ret = -ret;
		goto err_out;
	}

	fuse_reply_readlink(req, link);
	return;

err_out:
	fuse_reply_err(req, ret);
}

static void afs_fuse_mknod(fuse_req_t req, fuse_ino_t parent,
			const char *name, mode_t mode, dev_t rdev)
{
	int ret;
	uint64_t ino;
	struct fuse_entry_param entry;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	ret = afs_mode_validate(mode);
	if (ret)
		goto out_inval;

	if (S_ISDIR(mode) || S_ISLNK(mode))
		goto out_inval;

	if (afs_virtio_is_virtual(afs_virtio(ctx), parent)) {
		fuse_reply_err(req, EPERM);
		return;
	}

	memset(&entry, 0, sizeof(entry));

	afs_db_tx_begin(afs_db(ctx));

	ret = afs_db_inode_append(afs_db(ctx), parent, mode, rdev,
				&entry.attr, 0);
	if (ret)
		goto out_rollback;

	ino = entry.ino = entry.attr.st_ino;

	/** XXX: not sure about these timeout values */
	entry.attr_timeout = 2.0;
	entry.entry_timeout = 2.0;

	ret = afs_db_dirent_append(afs_db(ctx), parent, ino, name,
				S_ISDIR(mode), 0);
	if (ret)
		goto out_rollback;

	/**
	 * for direct interface, it is necessary to create object for
	 * consistency of obj id and inode # (regarding the collection)
	 */
	if (afs_super(ctx)->direct) {
		struct afs_filer_request fr;
		struct afs_stripe stripe;

		/** insert pathdb before create obj in the device */
		ret = pathdb_insert(ctx, ino);
		if (ret)
			goto out_rollback;

		stripe.stmode = AFS_STRIPE_NONE;
		stripe.stloc = -1;

		memset((void *) &fr, 0, sizeof(fr));
		fr.ino = ino;
		fr.stripe = &stripe;

		ret = afs_filer_handle_create(ctx, &fr);
		if (ret)
			goto out_rollback;

	}

	afs_db_tx_commit(afs_db(ctx));
	fuse_reply_entry(req, &entry);
	return;

out_inval:
	fuse_reply_err(req, EINVAL);
	return;

out_rollback:
	afs_db_tx_rollback(afs_db(ctx));
	fuse_reply_err(req, EIO);
	return;
}

static void afs_fuse_mkdir(fuse_req_t req, fuse_ino_t parent,
			const char *name, mode_t mode)
{
	int ret;
	uint64_t ino;
	struct fuse_entry_param entry;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	mode &= ALLPERMS;
	mode |= S_IFDIR;

	memset(&entry, 0, sizeof(entry));

	if (afs_virtio_is_virtual(afs_virtio(ctx), parent)) {
		fuse_reply_err(req, EPERM);
		return;
	}

	afs_db_tx_begin(afs_db(ctx));

	ret = afs_db_inode_append(afs_db(ctx), parent, mode, 0, &entry.attr,
				0);
	if (ret)
		goto err;

	ino = entry.ino = entry.attr.st_ino;

	/** XXX: not sure about these timeout values */
	entry.attr_timeout = 2.0;
	entry.entry_timeout = 2.0;

	ret = afs_db_dirent_append(afs_db(ctx), parent, ino, name, 1, 0);
	if (ret)
		goto err;

	afs_db_tx_commit(afs_db(ctx));
	fuse_reply_entry(req, &entry);
	return;

err:
	afs_db_tx_rollback(afs_db(ctx));
	fuse_reply_err(req, EIO);
}

/**
 * removing objects.
 *
 * XXX: we should consider the dependencies to keep the lineage data
 * consistent. we do not physically remove the object but keeps the metadata
 * even after link count of the object reaches 0. End data can be pruned upon
 * filesystem configuration which is made at creation time.
 *
 * TODO: the symlink table entries are not removed for now. do we need to?
 */
static void afs_fuse_unlink(fuse_req_t req, fuse_ino_t parent,
			const char *name)
{
	int ret;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	if (afs_virtio_is_virtual(afs_virtio(ctx), parent)) {
		fuse_reply_err(req, EPERM);
		return;
	}

	afs_db_tx_begin(afs_db(ctx));

	ret = afs_db_unlink(afs_db(ctx), parent, name);
	if (ret) {
		ret = -ret;
		goto out;
	}

#if 0
	/** any replicas should be deleted: this is done in the SQL by trigger
	 */
#endif
	afs_db_tx_commit(afs_db(ctx));
out:
	if (ret)
		afs_db_tx_rollback(afs_db(ctx));
	fuse_reply_err(req, ret);
}

static void afs_fuse_rmdir(fuse_req_t req, fuse_ino_t parent,
			const char *name)
{
	int ret;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	if (afs_virtio_is_virtual(afs_virtio(ctx), parent)) {
		fuse_reply_err(req, EPERM);
		return;
	}

	ret = afs_db_unlink(afs_db(ctx), parent, name);
	if (ret)
		ret = -ret;

	fuse_reply_err(req, ret);
}

static void afs_fuse_symlink(fuse_req_t req, const char *link,
			fuse_ino_t parent, const char *name)
{
	int ret;
	uint64_t ino;
	struct fuse_entry_param entry;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	memset(&entry, 0, sizeof(entry));
	entry.attr.st_size = strlen(link);

	afs_db_tx_begin(afs_db(ctx));

	ret = afs_db_inode_append(afs_db(ctx), parent,
			S_IFLNK | S_IRWXU | S_IRWXG | S_IRWXO, 0, &entry.attr,
			0);
	if (ret)
		goto eio;

	ino = entry.ino = entry.attr.st_ino;
	entry.attr_timeout = 2.0;
	entry.entry_timeout = 2.0;

	ret = afs_db_create_symlink(afs_db(ctx), ino, link);
	if (ret)
		goto eio;

	ret = afs_db_dirent_append(afs_db(ctx), parent, ino, name, 0, 0);
	if (ret)
		goto eio;

	afs_db_tx_commit(afs_db(ctx));
	fuse_reply_entry(req, &entry);
	return;

eio:
	afs_db_tx_rollback(afs_db(ctx));
	fuse_reply_err(req, EIO);
}

static void afs_fuse_rename(fuse_req_t req, fuse_ino_t parent,
			const char *name, fuse_ino_t newparent,
			const char *newname)
{
	int ret;
	int64_t ino = 0;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	if (parent == newparent && !strcmp(name, newname)) {
		fuse_reply_err(req, EINVAL);
		return;
	}

	afs_db_tx_begin(afs_db(ctx));

	ret = afs_db_dirent_remove(afs_db(ctx), parent, name, &ino, 1);
	if (ret) {
		ret = -ret;
		goto errout;
	}

	/** we don't have to recreate the '.' and '..' entries. */
	ret = afs_db_dirent_append(afs_db(ctx), newparent, ino, newname, 0, 0);
	if (ret) {
		ret = -ret;
		goto errout;
	}

	/** update the pathdb */
	ret = pathdb_update(ctx, ino);
	if (ret) {
		ret = -ret;
		goto errout;
	}

	afs_db_tx_commit(afs_db(ctx));
	fuse_reply_err(req, 0);
	return;

errout:
	afs_db_tx_rollback(afs_db(ctx));
	fuse_reply_err(req, ret);
}

static void afs_fuse_link(fuse_req_t req, fuse_ino_t ino,
			fuse_ino_t newparent, const char *newname)
{
	fuse_reply_err(req, ENOSYS);
}

static void afs_fuse_open(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
{
	int ret;
	struct afs_ctx *ctx = fuse_req_userdata(req);
	struct afs_stripe *stripe = (struct afs_stripe *)
						calloc(sizeof(*stripe), 1);

	if (!stripe) {
		fuse_reply_err(req, ENOMEM);
		return;
	}

	ret = afs_db_get_stripe_info(afs_db(ctx), ino, stripe);
	if (ret) {
		fuse_reply_err(req, EIO);
		return;
	}
	stripe->dirty = 0;

	fi->fh = (uint64_t) stripe;
	fi->direct_io = 0;
	fi->keep_cache = 1;
	fuse_reply_open(req, fi);
}

static void afs_fuse_read(fuse_req_t req, fuse_ino_t ino, size_t size,
			off_t off, struct fuse_file_info *fi)
{
	int ret;
	struct afs_ctx *ctx = fuse_req_userdata(req);
	struct afs_stripe *stripe = (struct afs_stripe *) fi->fh;
	struct stat *current = &stripe->stat;
	struct afs_filer_request r;

	/** is this file empty? */
	if (current->st_size == 0) {
		fuse_reply_buf(req, NULL, 0);
		return;
	}

	r.stripe = stripe;
	r.ino = ino;
	r.buf = malloc(size);
	r.size = size;
	r.off = off;

	if (!r.buf) {
		fuse_reply_err(req, ENOMEM);
		return;
	}

	/** this returns after reading is finished. */
	ret = afs_filer_handle_read(ctx, &r);
	if (ret)
		goto eio;

	if (stripe->stmode != AFS_STRIPE_VIRTIO &&
	    afs_config(ctx)->update_atime)
	{
		current->st_atime = afs_now();

		ret = afs_db_setattr(afs_db(ctx), ino, current);
		if (!ret)
			stripe->dirty = 0;
	}

	fuse_reply_buf(req, (char *) r.buf, size);
	free(r.buf);
	return;
eio:
	fuse_reply_err(req, EIO);
}

static void afs_fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
			size_t size, off_t off, struct fuse_file_info *fi)
{
	int ret;
	size_t newsize = 0;
	struct afs_ctx *ctx = fuse_req_userdata(req);
	struct afs_stripe *stripe = (struct afs_stripe *) fi->fh;
	struct stat *current = &stripe->stat;
	struct afs_filer_request r;

	r.stripe = stripe;
	r.ino = ino;
	r.buf = (void *) buf;
	r.size = size;
	r.off = off;

	/** check whether this is a virtual file */
	if (stripe->stmode == AFS_STRIPE_VIRTIO) {
	}

	/** this returns after writing is finished. */
	ret = afs_filer_handle_write(ctx, &r);
	if (ret)
		goto eio;

	if (stripe->stmode == AFS_STRIPE_VIRTIO)
		goto out;

	newsize = off + size;
	if (newsize > current->st_size) {
		current->st_size = newsize;
		stripe->dirty = 1;
	}

	if (afs_config(ctx)->update_mtime) {
		current->st_mtime = afs_now();
		stripe->dirty = 1;
	}

	afs_db_tx_begin(afs_db(ctx));

	/** any modification to the file should invalidate replicas. */
	ret = afs_db_invalidate_replica(afs_db(ctx), ino);
	if (ret)
		goto eio_rollback;

	if (stripe->dirty) {
		ret = afs_db_setattr(afs_db(ctx), ino, current);
		if (ret)
			goto eio_rollback;

		stripe->dirty = 0;
	}

	afs_db_tx_commit(afs_db(ctx));

out:
	fuse_reply_write(req, size);
	return;

eio_rollback:
	afs_db_tx_rollback(afs_db(ctx));
eio:
	fuse_reply_err(req, EIO);
}

static void afs_fuse_flush(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
{
	/**
	 * this is called upon close(2). do we need to do sth here?
	 */

	fuse_reply_err(req, 0);
}

static void afs_fuse_release(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
{
	int ret = 0;
	struct afs_ctx *ctx = fuse_req_userdata(req);
	struct afs_stripe *stripe = (struct afs_stripe *) fi->fh;

	/** check if we need to sync the metadata. */
	if (stripe->dirty) {
		struct stat *current = &stripe->stat;

		ret = afs_db_setattr(afs_db(ctx), ino, current);
		if (ret)
			ret = EIO;
	}

	free(stripe);
	fuse_reply_err(req, ret);
}

static void afs_fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
			struct fuse_file_info *fi)
{
	int ret = 0;
	struct afs_ctx *ctx = fuse_req_userdata(req);
	struct afs_stripe *stripe = (struct afs_stripe *) fi->fh;
	struct afs_filer_request r;

	r.stripe = stripe;
	r.ino = ino;

	ret = afs_filer_handle_fsync(ctx, &r);
	if (ret) {
		ret = EIO;
		goto out;
	}

	if (!datasync && stripe->dirty == 1) {
		struct stat *current = &stripe->stat;

		ret = afs_db_setattr(afs_db(ctx), ino, current);
		if (ret)
			ret = EIO;
	}
out:
	fuse_reply_err(req, ret);
}

static void afs_fuse_opendir(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
{
	int ret;
	struct afs_ctx *ctx = fuse_req_userdata(req);
	struct stat stbuf;

	ret = afs_db_getattr(afs_db(ctx), ino, &stbuf);
	if (ret < 0) {
		fuse_reply_err(req, ENOENT);
		return;
	}

	if (!S_ISDIR(stbuf.st_mode)) {
		fuse_reply_err(req, ENOTDIR);
		return;
	}

	fi->fh = (unsigned long) ret;
	fuse_reply_open(req, fi);
}

static void afs_fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
			off_t off, struct fuse_file_info *fi)
{
	int ret;
	struct dirbuf_iter di;
	struct afs_ctx *ctx = fuse_req_userdata(req);

	memset(&di, 0, sizeof(di));

	ret = afs_db_dirent_iter(afs_db(ctx), ino, afs_dirent_callback,
				&di);
	if (ret) {
		fuse_reply_err(req, EIO);
		return;
	}

	reply_buf_limited(req, di.buf.p, di.buf.size, off, size);
	free(di.buf.p);
}

static void afs_fuse_releasedir(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
{
	fuse_reply_err(req, 0);
}

static void afs_fuse_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
			struct fuse_file_info *fi)
{
	fuse_reply_err(req, 0);
}

static void afs_fuse_statfs(fuse_req_t req, fuse_ino_t ino)
{
	struct statvfs vfs;
	struct statfs fs;
	struct afs_ctx *ctx = fuse_req_userdata(req);

#if 0
	if (statfs(ctx->mountpoint, &fs) < 0) {
#endif
	/** FIXME: what is the correct thing to do here?? */
	if (statfs("/", &fs) < 0) {
		fuse_reply_err(req, errno);
		return;
	}

	memset(&vfs, 0, sizeof(vfs));
	vfs.f_bsize = fs.f_bsize;
	vfs.f_frsize = 512;
	vfs.f_blocks = fs.f_blocks;
	vfs.f_bfree = fs.f_bfree;
	vfs.f_bavail = fs.f_bavail;
	vfs.f_files = 0xfffffff;
	vfs.f_ffree = 0xffffff;
	vfs.f_fsid = afs_super(ctx)->magic;
	vfs.f_flag = 0;
	vfs.f_namemax = 256;

	fuse_reply_statfs(req, &vfs);
}

static void afs_fuse_setxattr(fuse_req_t req, fuse_ino_t ino,
			const char *name, const char *value, size_t size,
			int flags)
{
	fuse_reply_err(req, ENOSYS);
}

static void afs_fuse_getxattr(fuse_req_t req, fuse_ino_t ino,
			const char *name, size_t size)
{
	fuse_reply_xattr(req, 0);
}

static void afs_fuse_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
	fuse_reply_xattr(req, 0);
}

static void afs_fuse_removexattr(fuse_req_t req, fuse_ino_t ino,
			const char *name)
{
	fuse_reply_err(req, ENOSYS);
}

static void afs_fuse_access(fuse_req_t req, fuse_ino_t ino, int mask)
{
	/** XXX: no access controls for now. */
	fuse_reply_err(req, 0);
}

static void afs_fuse_create(fuse_req_t req, fuse_ino_t parent,
			const char *name, mode_t mode,
			struct fuse_file_info *fi)
{
	fuse_reply_err(req, ENOSYS);
}

static void afs_fuse_init(void *userdata, struct fuse_conn_info *conn)
{
	int i, ret = 0;
	int ndev = 0;
	char *devs[AFS_MAX_DEV];
	char *devstr, *token;
	struct afs_ctx *ctx = (struct afs_ctx *) userdata;
	struct afs_super *super;

	ret = afs_db_init(afs_db(ctx), afs_config(ctx)->dbfile);
	if (ret)
		abort();

	/** read the device information from db */
	super = afs_super(ctx);
	ret = afs_db_read_super(afs_db(ctx), super, 0);
	if (ret)
		abort();

	ndev = super->ndev;
	devstr = strdup(super->devs);

	i = 0;
	while (1) {
		token = strsep(&devstr, ",");
		if (!token)
			break;
		devs[i++] = token;
	}
	if (i != ndev)
		goto fail;

	/** initialize the device workers */
	ret = afs_osd_init(afs_osd(ctx), ndev, devs, super->direct, 100);
	if (ret)
		goto fail;

	/** initialize the scheduler */
	ret = afs_sched_init(afs_sched(ctx));
	if (ret)
		goto fail;

	/** initialize the virtual entries */
	ret = afs_virtio_init(afs_virtio(ctx));
	if (ret)
		goto fail;

	ret = afs_pathdb_init(afs_pathdb(ctx), afs_config(ctx)->pathdb_path);
	if (ret)
		goto fail;

	free(devstr);
	return;
fail:
	free(devstr);
	abort();
}

static void afs_fuse_destroy(void *userdata)
{
	int ret;
	struct afs_ctx *ctx = (struct afs_ctx *) userdata;

	ret = afs_remove_virtual_entries(ctx);

	afs_pathdb_exit(afs_pathdb(ctx));
	afs_osd_exit(afs_osd(ctx));
	afs_db_exit(afs_db(ctx));
	afs_config_exit(afs_config(ctx));
}

/**
 * activefs fuse operation table.
 *
 * (NI = Not Implemented, returns ENOSYS)
 */
static struct fuse_lowlevel_ops afs_ops = {
	.init		= afs_fuse_init,
	.destroy	= afs_fuse_destroy,
	.lookup		= afs_fuse_lookup,
	.forget		= afs_fuse_forget,		/* NI */
	.getattr	= afs_fuse_getattr,
	.setattr	= afs_fuse_setattr,
	.readlink	= afs_fuse_readlink,
	.mknod		= afs_fuse_mknod,
	.mkdir		= afs_fuse_mkdir,
	.unlink		= afs_fuse_unlink,
	.rmdir		= afs_fuse_rmdir,
	.symlink	= afs_fuse_symlink,
	.rename		= afs_fuse_rename,
	.link		= afs_fuse_link,
	.open		= afs_fuse_open,
	.read		= afs_fuse_read,
	.write		= afs_fuse_write,
	.flush		= afs_fuse_flush,
	.release	= afs_fuse_release,
	.fsync		= afs_fuse_fsync,
	.opendir	= afs_fuse_opendir,
	.readdir	= afs_fuse_readdir,
	.releasedir	= afs_fuse_releasedir,
	.fsyncdir	= afs_fuse_fsyncdir,
	.statfs		= afs_fuse_statfs,
	.setxattr	= afs_fuse_setxattr,		/* NI */
	.getxattr	= afs_fuse_getxattr,		/* NI */
	.listxattr	= afs_fuse_listxattr,		/* NI */
	.removexattr	= afs_fuse_removexattr,		/* NI */
	.access		= afs_fuse_access,
	.create		= afs_fuse_create,		/* NI */
};

/**
 * XXX we currently use statically located config file to receive filesystem
 * parameters. rewrite this to be a more general way.
 */
#define	AFS_CONFIG_FILE		"./activefs.conf"

int main(int argc, char **argv)
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_chan *ch;
	struct afs_ctx *ctx = &_ctx;
	char *mountpoint;
	int ret = -1;

	openlog("activefs", LOG_PID, LOG_LOCAL4);

	/** before starting the filesystem, check the config file */
	ret = afs_config_init(afs_config(ctx), AFS_CONFIG_FILE);
	if (ret) {
		fprintf(stderr, "failed to read configurations from %s.\n",
				AFS_CONFIG_FILE);
		return 1;
	}

	/** starting the filesystem.. */
	if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1
	    && (ch = fuse_mount(mountpoint, &args)) != NULL)
	{
		struct fuse_session *se;

		memset(ctx->mountpoint, 0, 512);
		strcpy(ctx->mountpoint, mountpoint);

		se = fuse_lowlevel_new(&args, &afs_ops,
					sizeof(afs_ops), ctx);
		if (se) {
			if (fuse_set_signal_handlers(se) != -1) {
				fuse_session_add_chan(se, ch);
				ret = fuse_session_loop(se);
				fuse_remove_signal_handlers(se);
				fuse_session_remove_chan(ch);
			}
			fuse_session_destroy(se);
		}
		fuse_unmount(mountpoint, ch);
	}
	fuse_opt_free_args(&args);

	return ret ? 1 : 0;
}

