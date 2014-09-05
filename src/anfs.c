/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * fuse implementation of activefs.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
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

#define FUSE_USE_VERSION		26
#include <fuse.h>

#include "anfs.h"

/** our private record for file operations. */
struct anfs_fh {
	int fd;		/* -1 on virtual entries */
	int location;	/* file location */
	uint64_t ino;
};

static struct anfs_ctx __ctx;
static struct anfs_ctx *ctx = &__ctx;

#define	anfs_fuse_ctx						\
		((struct anfs_ctx *) (fuse_get_context()->private_data))

/**
 * implementation of the fuse file operations.
 *
 * hs2: changes from the activefs implementation
 * - we don't use the filer/osd abstractions.
 * - we dont' use the low-level fuse api.
 */

static int anfs_getattr(const char *path, struct stat *stbuf)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_getattr(anfs_mdb(self), path, stbuf);
}

static int anfs_readlink(const char *path, char *link, size_t size)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_readlink(anfs_mdb(self), path, link, size);
}

static int anfs_mknod(const char *path, mode_t mode, dev_t dev)
{
	int ret;
	uint64_t ino;
	struct anfs_ctx *self = anfs_fuse_ctx;

	ret = anfs_mdb_mknod(anfs_mdb(self), path, mode, dev, &ino);
	if (ret)
		return ret;

	if (S_ISREG(mode))	/** backend file for regular file. */
		ret = anfs_store_create(anfs_store(self), ino, -1);

	return ret;
}

static int anfs_mkdir(const char *path, mode_t mode)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_mkdir(anfs_mdb(self), path, mode);
}

static int anfs_unlink(const char *path)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_unlink(anfs_mdb(self), path);
}

static int anfs_rmdir(const char *path)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_rmdir(anfs_mdb(self), path);
}

static int anfs_symlink(const char *path, const char *link)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_symlink(anfs_mdb(self), path, link);
}

static int anfs_rename(const char *old, const char *new)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_rename(anfs_mdb(self), old, new);
}

static int anfs_link(const char *path, const char *new)
{
	return -ENOSYS;
}

static int anfs_chmod(const char *path, mode_t mode)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_chmod(anfs_mdb(self), path, mode);
}

static int anfs_chown(const char *path, uid_t uid, gid_t gid)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_chown(anfs_mdb(self), path, uid, gid);
}

static int anfs_truncate(const char *path, off_t newsize)
{
	int ret, index;
	uint64_t ino;
	struct anfs_ctx *self = anfs_fuse_ctx;

	anfs_mdb_tx_begin(anfs_mdb(self));

	ret = anfs_mdb_truncate(anfs_mdb(self), path, newsize);
	if (ret)
		goto out_fail;

	ret = anfs_mdb_get_ino_loc(anfs_mdb(self), path, &ino, &index);
	if (ret)
		goto out_fail;

	ret = anfs_store_truncate(anfs_store(self), ino, index, newsize);
	if (ret)
		goto out_fail;

	anfs_mdb_tx_commit(anfs_mdb(self));
	return 0;

out_fail:
	anfs_mdb_tx_abort(anfs_mdb(self));
	return ret;
}

static int anfs_utime(const char *path, struct utimbuf *tbuf)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_utime(anfs_mdb(self), path, tbuf);
}

static int anfs_open(const char *path, struct fuse_file_info *fi)
{
	int ret, index;
	uint64_t ino;
	struct anfs_ctx *self = anfs_fuse_ctx;
	struct anfs_fh *handle = malloc(sizeof(*handle));

	if (!handle)
		return -ENOMEM;
	memset(handle, 0, sizeof(*handle));

	ret = anfs_mdb_get_ino_loc(anfs_mdb(self), path, &ino, &index);
	if (ret)
		return ret;

	handle->ino = ino;
	handle->location = index;

	if (ino >= ANFS_INO_NORMAL) {
		ret = anfs_store_open(anfs_store(self), ino, index, fi->flags);
		if (ret < 0)
			return -errno;
		handle->fd = ret;
	}
	else
		handle->fd = -1;

	fi->fh = (unsigned long) handle;

	return 0;
}

static int anfs_read(const char *path, char *buf, size_t size,
			off_t offset, struct fuse_file_info *fi)
{
	ssize_t ret;
	struct anfs_fh *handle = (struct anfs_fh *) fi->fh;
	__anfs_unused(path);

	/** if we implement multiple special files, we need to implement the
	 * read routine as well. */
	if (handle->fd < 0)
		return 0;
	else
		return pread(handle->fd, buf, size, offset);
}

static int anfs_write(const char *path, const char *buf, size_t size,
			off_t offset, struct fuse_file_info *fi)
{
	int ret;
	size_t written;
	struct stat stbuf;
	struct anfs_fh *handle = (struct anfs_fh *) fi->fh;
	struct anfs_ctx *self = anfs_fuse_ctx;

	if (handle->fd < 0) {
		uint64_t ino;

		switch (handle->ino) {
		case ANFS_INO_SUBMIT:
			ino = strtoul(buf, NULL, 0);
			ret = anfs_sched_submit_job(anfs_sched(ctx), ino);
			break;
		default:
			break;
		}

		if (ret) {
			errno = -EIO;
			return -1;
		}

		return size;
	}

	/** only normal files will fall here. */
	written = pwrite(handle->fd, buf, size, offset);
	if (written < 0)
		return ret;

	anfs_mdb_tx_begin(anfs_mdb(self));

	/** on the change of the file size, update the metadata */
	ret = anfs_mdb_getattr(anfs_mdb(self), path, &stbuf);
	if (ret)
		goto out_fail;

	if (stbuf.st_size < offset + written) {
		ret = anfs_mdb_truncate(anfs_mdb(self), path, offset+written);
		if (ret)
			goto out_fail;
	}

	anfs_mdb_tx_commit(anfs_mdb(self));
	return written;

out_fail:
	anfs_mdb_tx_abort(anfs_mdb(self));
	return ret;
}

static int anfs_statfs(const char *path, struct statvfs *vfs)
{
	int ret;
	struct statfs fs;

	if ((ret = statfs("/", &fs)) < 0)
		return ret;

	vfs->f_bsize = fs.f_bsize;
	vfs->f_frsize = 512;
	vfs->f_blocks = fs.f_blocks;
	vfs->f_bfree = fs.f_bfree;
	vfs->f_bavail = fs.f_bavail;
	vfs->f_files = 0xfffffff;
	vfs->f_ffree = 0xffffff;
	vfs->f_fsid = ANFS_MAGIC;
	vfs->f_flag = 0;
	vfs->f_namemax = 256;

	return 0;
}

/** this handler is called upon every close(2) */
static int anfs_flush(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

static int anfs_release(const char *path, struct fuse_file_info *fi)
{
	int ret = 0;
	struct anfs_fh *handle = (struct anfs_fh *) fi->fh;

	if (handle->fd > 0)
		ret = close(handle->fd);
	free(handle);

	return ret;
}

static int anfs_fsync(const char *path, int datasync,
			struct fuse_file_info *fi)
{
	struct anfs_fh *handle = (struct anfs_fh *) fi->fh;

	if (handle->fd < 0)
		return 0;

	return datasync ? fdatasync(handle->fd) : fsync(handle->fd);
}

/** TODO: implement the extended attributes */
static int anfs_setxattr(const char *path, const char *name,
				const char *value, size_t size, int flags)
{
	return -ENOSYS;
}

static int anfs_getxattr(const char *path, const char *name,
				char *value, size_t size)
{
	return -ENOSYS;
}

static int anfs_listxattr(const char *path, char *list, size_t size)
{
	return -ENOSYS;
}

static int anfs_removexattr(const char *path, const char *name)
{
	return -ENOSYS;
}

/** readdir implementation is stateless. nothing to do here. */
static int anfs_opendir(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

static int anfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
				off_t offset, struct fuse_file_info *fi)
{
	int ret;
	uint64_t pos = offset;
	struct anfs_dirent dirent;
	struct anfs_ctx *self = anfs_fuse_ctx;

	/** TODO: this is not efficient since it fetches entries one by one.
	 */
	do {
		ret = anfs_mdb_readdir(anfs_mdb(self), path, pos, &dirent);
		if (ret == 0)
			break;
		else if (ret < 0)
			return ret;
		else {
			ret = filler(buf, dirent.d_name, &dirent.stat, 0);
			pos++;
		}
	} while (ret == 0);

	return 0;
}

static int anfs_releasedir(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

static int anfs_fsyncdir(const char *path, int datasync,
					struct fuse_file_info *fi)
{
	return 0;
}

static void *anfs_init(struct fuse_conn_info *conn)
{
	int i, ret;
	int ndev = 0;
	char *devs[ANFS_MAX_DEV];
	char *devstr, *token;
	struct anfs_super *super;
	struct anfs_ctx *self = ctx;

	/** initialize the db module and read superblock from db */
	ret = anfs_mdb_init(anfs_mdb(self), anfs_config(self)->dbfile);
	if (ret)
		goto out_err;

	super = anfs_super(self);
	ret = anfs_mdb_read_super(anfs_mdb(self), super, 0);
	if (ret)
		goto out_err;

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
		goto out_err;

	/** initialize the rest of the modules */
	ret = anfs_osd_init(anfs_osd(self), ndev, devs,
			anfs_config(self)->worker_idle_sleep);
	if (ret)
		goto out_err;
	ret = anfs_sched_init(anfs_sched(self));
	if (ret)
		goto out_err;
	ret = anfs_store_init(anfs_store(self), ndev, devs);
	if (ret)
		goto out_err;
	ret = anfs_pathdb_init(anfs_pathdb(self),
				anfs_config(self)->pathdb_path);
	if (ret)
		goto out_err;

	free(devstr);
	return self;

out_err:
	exit(1);
}

static void anfs_destroy(void *context)
{
	struct anfs_ctx *self = anfs_fuse_ctx;

	anfs_pathdb_exit(anfs_pathdb(self));
	anfs_store_exit(anfs_store(self));
	anfs_sched_exit(anfs_sched(self));
	anfs_osd_exit(anfs_osd(self));
	anfs_mdb_exit(anfs_mdb(self));
	anfs_config_exit(anfs_config(self));
}

static int anfs_access(const char *path, int mask)
{
	/** TODO: implement this */
	return 0;
}

static int anfs_ftruncate(const char *path, off_t newsize,
				struct fuse_file_info *fi)
{
	int ret = 0;
	struct anfs_fh *handle = (struct anfs_fh *) fi->fh;
	struct anfs_ctx *self = anfs_fuse_ctx;

	if (handle->fd < 0)
		return 0;	/** do nothing for special files */

	anfs_mdb_tx_begin(anfs_mdb(self));

	ret = anfs_mdb_truncate(anfs_mdb(self), path, newsize);
	if (ret)
		goto out_fail;

	ret = ftruncate(handle->fd, newsize);
	if (ret)
		goto out_fail;

	anfs_mdb_tx_commit(anfs_mdb(self));

	return ret;

out_fail:
	anfs_mdb_tx_abort(anfs_mdb(self));
	return ret;
}

static int anfs_fgetattr(const char *path, struct stat *stbuf,
				struct fuse_file_info *fi)
{
	struct anfs_ctx *self = anfs_fuse_ctx;
	return anfs_mdb_getattr(anfs_mdb(self), path, stbuf);
}

static int anfs_utimens(const char *path, const struct timespec tv[2])
{
	return -ENOSYS;
}


static struct fuse_operations anfs_ops = {
	.getattr	= anfs_getattr,
	.readlink	= anfs_readlink,
	.getdir		= NULL,
	.mknod		= anfs_mknod,
	.mkdir		= anfs_mkdir,
	.unlink		= anfs_unlink,
	.rmdir		= anfs_rmdir,
	.symlink	= anfs_symlink,
	.rename		= anfs_rename,
	.link		= anfs_link,
	.chmod		= anfs_chmod,
	.chown		= anfs_chown,
	.truncate	= anfs_truncate,
	.utime		= anfs_utime,
	.open		= anfs_open,
	.read		= anfs_read,
	.write		= anfs_write,
	.statfs		= anfs_statfs,
	.flush		= anfs_flush,
	.release	= anfs_release,
	.fsync		= anfs_fsync,
	.setxattr	= anfs_setxattr,
	.getxattr	= anfs_getxattr,
	.listxattr	= anfs_listxattr,
	.removexattr	= anfs_removexattr,
	.opendir	= anfs_opendir,
	.readdir	= anfs_readdir,
	.releasedir	= anfs_releasedir,
	.fsyncdir	= anfs_fsyncdir,
	.init		= anfs_init,
	.destroy	= anfs_destroy,
	.access		= anfs_access,
	.create		= NULL,
	.ftruncate	= anfs_ftruncate,
	.fgetattr	= anfs_fgetattr,
	.lock		= NULL,
#if 0
	/** currently, stick to deprecated utime(2) */
	.utimens	= anfs_utimens,
#endif
	.bmap		= NULL,
};

/**
 * main program
 */

enum {
	OPTKEY_DEBUG	= 0,
	OPTKEY_CONFIG,
	OPTKEY_HELP
};

static struct fuse_opt anfs_fuse_opts[] = {
	FUSE_OPT_KEY("-d", OPTKEY_DEBUG),
	FUSE_OPT_KEY("--config=%s", OPTKEY_CONFIG),
	FUSE_OPT_KEY("-c %s", OPTKEY_CONFIG),
	FUSE_OPT_KEY("-h", OPTKEY_HELP),
	FUSE_OPT_END
};

#define	ANFS_DEFAULT_CONF_FILE		"./anfs.conf"

static int anfs_debug;
static const char *anfs_conf_file;
static const char *anfs_root;

static void usage(void)
{
	printf(
	"Usage: anfs [options] mountpoint\n"
	"\n"
	"options:\n"
	"    -d                  Enable debug output\n"
	"    -c, --config=<file> Configuration path (default ./anfs.conf)\n"
	"    -o opt,[opt...]     mount options\n"
	"    -h, --help          print help\n\n");
}

static inline char *arg_parse_strval(const char *arg)
{
	if (arg[1] == '-')
		return strdup(&arg[2]);
	else
		return strdup(strchr(arg, '='));
}

static int anfs_process_opt(void *data, const char *arg, int key,
				struct fuse_args *outargs)
{
	switch (key) {
	case OPTKEY_DEBUG:
		anfs_debug = 1;
		return 1;
	case OPTKEY_CONFIG:
		anfs_conf_file = arg_parse_strval(arg);
		break;
	case OPTKEY_HELP:
		usage();
		exit(1);
	default:
		if (arg[0] != '-') {
			/** this is a mount point */
			anfs_root = strdup(arg);
		}
		return 1;
	}

	return 0;
}

int main(int argc, char **argv)
{
	int ret;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	if (fuse_opt_parse(&args, NULL, anfs_fuse_opts, anfs_process_opt) < 0)
		return -EINVAL;

#ifdef FUSE_CAP_BIG_WRITES
	if (fuse_opt_add_arg(&args, "-obig_writes")) {
		fprintf(stderr, "failed to enable big writes!\n");
		return -EINVAL;
	}
#endif

	if (fuse_opt_add_arg(&args, "-ouse_ino")) {
		fprintf(stderr, "failed to enable inode use!\n");
		return -EINVAL;
	}

	if (!anfs_conf_file)
		anfs_conf_file = ANFS_DEFAULT_CONF_FILE;

	ret = anfs_config_init(anfs_config(ctx), anfs_conf_file);
	if (ret) {
		fprintf(stderr, "failed to parse the configuration file (%s)\n",
				anfs_conf_file);
		return 1;
	}

	ctx->root = anfs_root;

	return fuse_main(args.argc, args.argv, &anfs_ops, ctx);
}

