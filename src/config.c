/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * filesystem configurations.
 */
#include <ctype.h>
#include "anfs.h"

static const int CFGLBUFSZ = 1024;

static inline char *get_param_str(char *line)
{
	char *pos = strchr(line, '=');

	++pos;
	while (isspace(*pos))
		pos++;

	return pos;
}

static inline int read_sched_policy(const char *str)
{
#if 0
	if (!strncmp(str, "input", strlen("input")))
		return AFS_SCHED_POLICY_INPUT;
	else
		return AFS_SCHED_POLICY_RR;
#endif

	/** TODO: rewrite this function once you add more policies */
	return 0;
}

int anfs_config_init(struct anfs_config *self, const char *cfile)
{
	FILE *fp;
	int ret = 0;
	char linebuf[CFGLBUFSZ];
	char *current, *param;

	if (!self)
		return -EINVAL;

	fp = fopen(cfile, "r");
	if (!fp)
		return -errno;

	while (fgets(linebuf, CFGLBUFSZ-1, fp) != NULL) {
		if (linebuf[0] == '#' || strempty(linebuf))
			continue;

		current = strtrim(linebuf);

		if (strncmp(current, "meta_dbfile", strlen("meta_dbfile"))
				== 0)
		{
			param = get_param_str(current);
			self->dbfile = strdup(param);
		}
		else if (strncmp(current, "update_mtime",
					strlen("update_mtime")) == 0)
		{
			param = get_param_str(current);
			self->update_mtime = atoi(param) == 0 ? 0 : 1;
		}
		else if (strncmp(current, "update_atime",
					strlen("update_atime")) == 0)
		{
			param = get_param_str(current);
			self->update_atime = atoi(param) == 0 ? 0 : 1;
		}
		else if (strncmp(current, "worker_idle_sleep",
					strlen("worker_idle_sleep")) == 0)
		{
			param = get_param_str(current);
			self->worker_idle_sleep = strtol(param, NULL, 0);
		}
		else if (strncmp(current, "sched_policy",
					strlen("sched_policy")) == 0)
		{
			param = get_param_str(current);
			self->sched_policy = read_sched_policy(param);
		}
		else if (strncmp(current, "pathdb_path",
					strlen("pathdb_path")) == 0)
		{
			param = get_param_str(current);
			self->pathdb_path = strdup(param);;
		}
		else {
			ret = -EINVAL;	/** unknown option */
			goto out_close;
		}
	}
	if (ferror(fp)) {
		ret = -errno;
		goto out_close;
	}

	if (!self->dbfile) {
		ret = -EINVAL;
		goto out_close;
	}

	self->configfile = strdup(cfile);

out_close:
	fclose(fp);
	return ret;
}

