/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * fuse implementation of activefs.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
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

#include "activefs.h"

static struct afs_ctx __ctx;
static struct afs_ctx *ctx = &__ctx;

/**
 * implementation of the fuse file operations.
 */

static int anfs_getattr(const char *path, struct stat *stbuf)
{
}

static int anfs_readlink(const char *path, char *link, size_t size)
{
}

static int anfs_mknod(const char *path, mode_t mode, dev_t dev)
{
}

static int anfs_mkdir(const char *path, mode_t mode)
{
}

static int anfs_unlink(const char *path)
{
}

static int anfs_rmdir(const char *path)
{
}

static int anfs_symlink(const char *path, const char *link)
{
}

static int anfs_rename(const char *old, const char *new)
{
}

static int anfs_link(const char *path, const char *new)
{
}

static int anfs_chmod(const char *path, mode_t mode)
{
}

static int anfs_chown(const char *path, uid_t uid, gid_t gid)
{
}

static int anfs_truncate(const char *path, off_t newsize)
{
}

static int anfs_utime(const char *path, struct utimbuf *tbuf)
{
}

static int anfs_open(const char *path, struct fuse_file_info *fi)
{
}

static int anfs_read(const char *path, char *buf, size_t size,
				off_t offset, struct fuse_file_info *fi)
{
}

static int anfs_write(const char *path, const char *buf, size_t size,
				off_t offset, struct fuse_file_info *fi)
{
}

static int anfs_statfs(const char *path, struct statvfs *vfs)
{
}

static int anfs_flush(const char *path, struct fuse_file_info *fi)
{
}

static int anfs_release(const char *path, struct fuse_file_info *fi)
{
}

static int anfs_release(const char *path, struct fuse_file_info *fi)
{
}

static int anfs_setxattr(const char *path, const char *name,
				const char *value, size_t size, int flags)
{
}

static int anfs_getxattr(const char *path, const char *name,
				char *value, size_t size)
{
}

static int anfs_listxattr(const char *path, char *list, size_t size)
{
}

static int anfs_removexattr(const char *path, const char *name)
{
}

/**
 *  * our implementation of readdir is stateless.
 *   */

static int anfs_opendir(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

static int anfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
				off_t offset, struct fuse_file_info *fi)
{
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
}

static void anfs_destroy(void *context)
{
}

static int anfs_access(const char *path, int mask)
{
	/** TODO: implement this */
	return 0;
}

static int anfs_ftruncate(const char *path, off_t newsize,
					struct fuse_file_info *fi)
{
}

static int anfs_fgetattr(const char *path, struct stat *stbuf,
					struct fuse_file_info *fi)
{
}

static int anfs_utimens(const char *path, const struct timespec tv[2])
{
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

#if 0
	/** not necessary for now */
	int uid, gid;
	uid = getuid();
	gid = getgid();
#endif

	if (fuse_opt_parse(&args, NULL, anfs_fuse_opts, anfs_process_opt) < 0)
		return -EINVAL;

#ifdef FUSE_CAP_BIG_WRITES
	if (fuse_opt_add_arg(&args, "-obig_writes")) {
		fprintf(stderr, "failed to enable big writes!\n");
		return -EINVAL;
	}
#endif

	if (!anfs_conf_file)
		anfs_conf_file = ANFS_DEFAULT_CONF_FILE;

	ret = afs_config_init(afs_config(ctx), anfs_conf_file);
	if (ret) {
		fprintf(stderr, "failed to parse the configuration file (%s)\n",
				anfs_conf_file);
		return 1;
	}

	ctx->root = anfs_root;

	return fuse_main(args.argc, args.argv, &anfs_ops, __ctx);
}

