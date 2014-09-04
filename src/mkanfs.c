/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * create/initialize a new activefs.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <getopt.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sqlite3.h>
#include <open-osd/libosd.h>

#define	ANFS_MAX_DEV		16

/**
 * filesystem params.
 */
static int ndev;
static char *devs[ANFS_MAX_DEV];
static int stmode;
static uint64_t stsize = (1<<20);	/** default striping: 1MB */
static uint64_t stwidth;		/** default width = ndev */
static uint64_t partition = 0x22222;
static int direct;

/**
 * database stuffs.
 */
static sqlite3 *db;
static const char *dbpath = "/tmp/anfs.db";
static char sqlbuf[2048];
extern char *create_sql;

/**
 * program options.
 */
static const char *usage_str = 
"\n"
"Usage: mkfs.anfs [option]...\n"
"Creates and initializes anfs.\n\n"
"  -d, --device=<path>        An active osd device to be used.\n"
"                             Multiple options can be used for specifying\n"
"                             multiple devices.\n"
"  -f, --file=<path>          Specifiy path for storing database file. If \n"
"                             not specified, /tmp/afs.db is used.\n"
"  -p, --partition=<pid>      Partition number to be used. Default 0x22222.\n"
"  -r, --raw-device           Use direct osd library (rather than exofs).\n"
"  -m, --stripe-mode=<name>   Stripe mode, one of the followings:\n"
"                               none, static\n"
"  -s, --stripe-size=<bytes>  Default stripe size to be used for multiple\n"
"                             devices. 1 MB, by default.\n"
"  -w, --stripe-width=<width> Default stripe width to be used for multiple\n"
"                             devices. All available devices are used by\n"
"                             default.\n"
"  -h, --help                 Shows this help message.\n"
"\n";

static struct option opt[] = {
	{ .name = "device", .has_arg = 1, .flag = NULL, .val = 'd' },
	{ .name = "file", .has_arg = 1, .flag = NULL, .val = 'f' },
	{ .name = "partition", .has_arg = 1, .flag = NULL, .val = 'p' },
	{ .name = "raw-device", .has_arg = 0, .flag = NULL, .val = 'r' },
	{ .name = "stripe-mode", .has_arg = 1, .flag = NULL, .val = 'm' },
	{ .name = "stripe-size", .has_arg = 1, .flag = NULL, .val = 's' },
	{ .name = "stripe-width", .has_arg = 1, .flag = NULL, .val = 'w' },
	{ .name = "help", .has_arg = 0, .flag = NULL, .val = 'h' },
	{ 0, 0, 0, 0 },
};

#ifndef	llu
#define llu(x)		((unsigned long long) (x))
#endif

static void add_device(const char *path)
{
	devs[ndev] = strdup(path);
	if (devs[ndev] == NULL) {
		perror("strdup failed");
		assert(0);
	}

	ndev++;
}

static void osdblk_make_credential(u8 *creds, struct osd_obj_id *obj,
				   bool is_v1)
{
	osd_sec_init_nosec_doall_caps(creds, obj, false, is_v1);
}

static int osdblk_exec(struct osd_request *or, u8 *cred)
{
	struct osd_sense_info osi;
	int ret;

	ret = osd_finalize_request(or, 0, cred, NULL);
	if (ret)
		return ret;

	osd_execute_request(or);
	ret = osd_req_decode_sense(or, &osi);

	if (ret) { /* translate to Linux codes */
		if (osi.additional_code == scsi_invalid_field_in_cdb) {
			if (osi.cdb_field_offset == OSD_CFO_STARTING_BYTE)
				ret = 0; /*this is OK*/
			if (osi.cdb_field_offset == OSD_CFO_OBJECT_ID)
				ret = -ENOENT;
			else
				ret = -EINVAL;
		} else if (osi.additional_code == osd_quota_error)
			ret = -ENOSPC;
		else
			ret = -EIO;
	}

	return ret;
}

static int create_partition(void)
{
	int i, ret = 0;
	struct osd_dev *osd;
	struct osd_request *or;
	u8 creds[OSD_CAP_LEN];
	struct osd_obj_id obj = { .partition = partition, .id = 0 };

	for (i = 0; i < ndev; i++) {
		ret = osd_open(devs[i], &osd);
		if (ret)
			return ret;

		or = osd_start_request(osd, GFP_KERNEL);
		if (unlikely(!or))
			return -ENOMEM;

		osdblk_make_credential(creds, &obj, osd_req_is_ver1(or));
		osd_req_create_partition(or, partition);
		ret = osdblk_exec(or, creds);
		osd_end_request(or);
		osd_close(osd);

		if (ret)
			break;
	}

	return ret;
}

static int create_directories(void)
{
	int i, id;
	int ret;

	for (i = 0; i < ndev; i++) {
		sprintf(sqlbuf, "%s/objects", devs[i]);
		ret = mkdir(sqlbuf, 0755);
		if (ret) {
			perror("mkdir");
			return ret;
		}

		sprintf(sqlbuf, "%s/jobs", devs[i]);
		ret = mkdir(sqlbuf, 0755);
		if (ret) {
			perror("mkdir");
			return ret;
		}

		for (id = 0; id < 0x100; id++) {
			sprintf(sqlbuf, "%s/objects/%02x", devs[i], id);
			ret = mkdir(sqlbuf, 0755);
			if (ret) {
				perror("mkdir");
				return ret;
			}
		}
	}

	return 0;
}

static inline
int create_dirent(sqlite3_stmt *stmt, uint64_t parent, uint64_t ino,
		const char *name)
{
	sqlite3_reset(stmt);
	sqlite3_bind_int64(stmt, 1, parent);
	sqlite3_bind_int64(stmt, 2, ino);
	sqlite3_bind_text(stmt, 3, name, -1, SQLITE_STATIC);

	return sqlite3_step(stmt);
}

static int do_mkfs(void)
{
	int i, ret = 0;
	uint64_t root_ino, jobs_ino, running_ino, failed_ino, submit_ino;
	char devstr[1024], *pos;
	struct timeval current_time;
	sqlite3_stmt *stmt;

	/** open db connection */
	ret = sqlite3_open(dbpath, &db);
	if (ret) {
		fprintf(stderr, "failed to open database.\n");
		goto out;
	}

	/** create tables */
	ret = sqlite3_exec(db, create_sql, NULL, NULL, NULL);
	if (ret)
		goto out_err;

	gettimeofday(&current_time, NULL);

	ret = sqlite3_exec(db, "begin transaction;", NULL, NULL, NULL);
	if (ret != SQLITE_OK)
		goto out_err;

	/** write the superblock */
	printf("writing the superblock...\n");
	sprintf(sqlbuf, "insert into anfs_super "
		"(version,ctime,ndev,devs,root,i_submit)"
		"values (?,?,?,?,?,?)");

	ret = sqlite3_prepare_v2(db, sqlbuf, -1, &stmt, NULL);
	if (ret != SQLITE_OK)
		goto out_err;

	pos = devstr;
	for (i = 0; i < ndev; i++)
		pos += sprintf(pos, "%s,", devs[i]);
	*--pos = '\0';

	sqlite3_bind_int(stmt, 1, 1);
	sqlite3_bind_int(stmt, 2, current_time.tv_sec);
	sqlite3_bind_int(stmt, 3, ndev);
	sqlite3_bind_text(stmt, 4, devstr, -1, SQLITE_STATIC);
	sqlite3_bind_int64(stmt, 5, 1);
	sqlite3_bind_int64(stmt, 6, 2);

	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE)
		goto out_err;
	sqlite3_finalize(stmt);

	/** create data directories for exofs mode */
	printf("preparing data storage...\n");
	if (direct)
		ret = create_partition();
	else
		ret = create_directories();

	ret = sqlite3_exec(db, "end transaction;", NULL, NULL, NULL);
	goto out_close;

out_err:
	fprintf(stderr, "dberr: %s\n", sqlite3_errmsg(db));

out_close:
	ret = sqlite3_close(db);
	if (ret != SQLITE_OK)
		fprintf(stderr, "%s\n", sqlite3_errmsg(db));
out:
	return ret;
}

int main(int argc, char **argv)
{
	int ret = 0;
	int ch;

	while ((ch = getopt_long(argc, argv, "d:f:p:rs:w:h", opt, NULL))
			!= -1)
	{
		switch (ch) {
		case 'd':
			add_device(optarg);
			break;
		case 'f':
			dbpath = optarg;
			break;
		case 'p':
			partition = strtol(optarg, NULL, 0);
			break;
		case 'r':
			direct = 1;
			break;
		case 'm': {
			if (0 == strncmp(optarg, "none", strlen("none"))) {
				stmode = 0;
			}
			else if (0 ==
				strncmp(optarg, "static", strlen("static")))
			{
				stmode = 1;
			}
			else {
				fprintf(stderr,
					"%s is not understood\n", optarg);
				goto usage_exit;
			}
			  }
			break;
		case 's':
			stsize = strtol(optarg, NULL, 0);
			break;
		case 'w':
			stwidth = strtol(optarg, NULL, 0);
			break;
		case 'h':
		default:
usage_exit:
			fputs(usage_str, stderr);
			return 0;
		}
	}

	if (ndev == 0) {
		fputs("At least one device has to be specified!\n", stderr);
		fputs(usage_str, stderr);
		return 1;
	}

	if (ndev > ANFS_MAX_DEV) {
		fprintf(stderr, "Sorry, the maximum number of device is %d\n",
			ANFS_MAX_DEV);
		return 1;
	}

	if (stwidth > (uint64_t) ndev) {
		fprintf(stderr, "Stripe width cannot be greater than the "
				"number of devices!\n");
		return 1;
	}

	printf("partition no = 0x%llx\n", llu(partition));
	printf("stripe size  = %llu bytes\n", llu(stsize));
	printf("stripe width = %llu\n", llu(stwidth));
	printf("db file      = %s\n", dbpath);
	printf("devices      = { ");

	for (ch = 0; ch < ndev; ch++) {
		printf("%s ", devs[ch]);
	}
	printf("}\npress ENTER to continue..\n");
	ch = getchar();

	ret = do_mkfs();

	if (ret)
		printf("\nfaild to create filesystem (%d).\n", ret);
	else
		printf("\nfilesystem created successfully.\n");

	return ret;
}

