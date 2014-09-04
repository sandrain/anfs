/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * parse the job script and build data structures for scheduler.
 */
#include <errno.h>
#include <libconfig.h>
#include "anfs.h"

struct anfs_parser_data {
	config_t config;
	struct anfs_job *current;
	const char *script;
	const char *bindir;
	const char *datadir;
	anfs_htable datahash;
};

static inline void cleanup_task(struct anfs_task *t)
{
	if (t->name && t->name != t->kernel)
		free((void *) t->name);
	if (t->kernel)
		free((void *) t->kernel);
	if (t->argument)
		free((void *) t->argument);
	free(t);
}

static inline void cleanup_task_data(struct anfs_task *t)
{
	int i;
	struct anfs_task_data *td;
	struct anfs_data_file *df;

	if (t->input) {
		td = t->input;
		free(td);
	}
	if (t->output) {
		td = t->output;
		for (i = 0; i < td->n_files; i++) {
			df = td->files[i];
			if (df) {
				free(df);
				df = NULL;
			}
		}
		free(td);
	}
}

static inline void cleanup_success(struct anfs_parser_data *self)
{
	anfs_hash_exit(&self->datahash);
	config_destroy(&self->config);
}

static inline void cleanup_fail(struct anfs_parser_data *self)
{
	if (self->current)
		anfs_parser_cleanup_job(self->current);

	cleanup_success(self);
}

static const char *get_anfs_bin_path(struct anfs_parser_data *self, const char *s)
{
	char *sb;

	if (s[0] == '/')
		return anfs_strdup(s);

	sb = anfs_calloc(1, strlen(self->bindir) + strlen(s) + 1);
	sprintf(sb, "%s%s", self->bindir, s);

	return sb;
}

static const char *get_anfs_data_path(struct anfs_parser_data *self,
					const char *s)
{
	char *sb;

	if (s[0] == '/')
		return anfs_strdup(s);

	sb = anfs_calloc(1, strlen(self->datadir) + strlen(s) + 1);
	sprintf(sb, "%s%s", self->datadir, s);

	return sb;
}

static struct anfs_task *alloc_tasklet(struct anfs_parser_data *self,
				struct anfs_job *job, config_setting_t *task)
{
	int ret = 0;
	long affinity = -1;
	const char *tmp;
	struct anfs_task *tasklet = anfs_calloc(1, sizeof(*tasklet));

	tasklet->job = job;

	ret = config_setting_lookup_string(task, "kernel", &tmp);
	if (ret != CONFIG_TRUE)
		goto out_free;
	tasklet->kernel = get_anfs_bin_path(self, tmp);

	ret = config_setting_lookup_string(task, "name", &tmp);
	if (ret != CONFIG_TRUE)
		tasklet->name = tasklet->kernel;
	else
		tasklet->name = anfs_strdup(tmp);

	ret = config_setting_lookup_string(task, "argument", &tmp);
	if (ret == CONFIG_TRUE)
		tasklet->argument = anfs_strdup(tmp);

	ret = config_setting_lookup_int(task, "affinity", &affinity);
	tasklet->affinity = affinity;

	return tasklet;

out_free:
	if (tasklet) {
		cleanup_task(tasklet);
		tasklet = NULL;
	}
	return tasklet;
}

static int alloc_tasklet_input(struct anfs_parser_data *self,
			config_setting_t *task, struct anfs_task *tasklet)
{
	int i;
	int count;
	const char *tmp;
	struct anfs_data_file **file, *dt;
	config_setting_t *input = config_setting_get_member(task, "input");

	if (!input) {
		/**
		 * the task doesn't have any input.
		 */
		tasklet->input = anfs_calloc(1, sizeof(struct anfs_task_data));
		tasklet->input->n_files = 0;
		return 0;
	}

	count = config_setting_length(input);
	tasklet->input = anfs_calloc(1, sizeof(struct anfs_task_data) +
					count * sizeof(struct anfs_data_file *));
	tasklet->input->n_files = count;

	for (i = 0; i < count; i++) {
		tmp = config_setting_get_string_elem(input, i);
		if (!tmp)
			return -2;

		tmp = get_anfs_data_path(self, tmp);
		dt = anfs_hash_search(&self->datahash, tmp);

		if (dt)
			tasklet->input->files[i] = dt;
		else {
			file = &tasklet->input->files[i];
			*file = anfs_calloc(1, sizeof(struct anfs_data_file));
			(*file)->path = tmp;
			(*file)->producer = NULL;
		}
	}

	return 0;
}

/**
 * no need to bother resolving dependencies here. just allocate data for each
 * output entry.
 */
static int alloc_tasklet_output(struct anfs_parser_data *self,
			config_setting_t *task, struct anfs_task *tasklet)
{
	int i;
	int count;
	const char *tmp;
	struct anfs_data_file **file;
	config_setting_t *output = config_setting_get_member(task, "output");

	if (!output)
		return -1;

	count = config_setting_length(output);

	tasklet->output = anfs_calloc(1, sizeof(struct anfs_task_data) +
				count * sizeof(struct anfs_data_file *));
	tasklet->output->n_files = count;

	for (i = 0; i < count; i++) {
		file = &tasklet->output->files[i];
		tmp = config_setting_get_string_elem(output, i);
		tmp = get_anfs_data_path(self, tmp);

		if (!tmp)
			return -2;

		(*file) = anfs_calloc(1, sizeof(struct anfs_data_file));
		(*file)->path = tmp;
		(*file)->producer = tasklet;

		anfs_hash_insert(&self->datahash, tmp, *file);
	}

	return 0;
}

static inline int alloc_tasklet_ios(struct anfs_parser_data *self, 
			config_setting_t *task, struct anfs_task *tasklet)
{
	int ret;

	ret = alloc_tasklet_input(self, task, tasklet);
	if (ret)
		return ret;

	ret = alloc_tasklet_output(self, task, tasklet);
	return ret;
}

#if 0
static int process_argument(struct anfs_parser_data *self, struct anfs_task *task)
{
	return 0;
}
#endif

static int build_tasks(struct anfs_parser_data *self, struct anfs_job *job,
			config_setting_t *task, struct list_head **link)
{
	int ret;
	struct anfs_task *tasklet;

	tasklet = alloc_tasklet(self, job, task);
	if (!tasklet)
		return 0;

	ret = alloc_tasklet_ios(self, task, tasklet);
	if (ret) {
		cleanup_task(tasklet);
		return 0;
	}

#if 0
	ret = process_argument(self, tasklet);
	if (ret)
		return 0;
#endif

	*link = &tasklet->list;
	return 1;
}

static int parse_script(struct anfs_parser_data *self)
{
	int i, ret, count, sched = 0;
	int n_tasks = 0;
	const char *tmp;
	struct list_head *link;
	config_setting_t *tasks;
	config_t *config = &self->config;
	struct anfs_job *job = self->current;

	if (config_lookup_string(config, "name", &tmp))
		job->name = anfs_strdup(tmp);
	if (config_lookup_string(config, "bindir", &tmp))
		self->bindir = tmp;
	if (config_lookup_string(config, "datadir", &tmp))
		self->datadir = tmp;
	if (config_lookup_string(config, "schedule", &tmp)) {
		if (strncmp(tmp, "rr", strlen("rr")) == 0)
			sched = ANFS_SCHED_POLICY_RR;
		else if (strncmp(tmp, "input", strlen("input")) == 0)
			sched = ANFS_SCHED_POLICY_INPUT;
		else if (strncmp(tmp, "minwait", strlen("minwait")) == 0)
			sched = ANFS_SCHED_POLICY_MINWAIT;
		else
			sched = 0;	/** default */
	}

	tasks = config_lookup(config, "tasks");
	if (tasks == NULL)
		return 0;

	count = config_setting_length(tasks);

	for (i = 0; i < count; i++) {
		config_setting_t *task = config_setting_get_elem(tasks, i);
		ret = build_tasks(self, job, task, &link);
		if (ret > 0) {
			job->n_tasks++;
			list_add_tail(link, &job->task_list);
		}
	}

	job->n_tasks = count;
	job->sched = sched;

	return n_tasks;
}

int anfs_parser_parse_script(const char *buf, size_t len, struct anfs_job **job)
{
	int ret;
	FILE *stream;
	struct anfs_parser_data self;

	self.current = anfs_calloc(1, sizeof(*self.current));
	if (anfs_hash_init(100, &self.datahash) == NULL)
		return -errno;

	self.script = buf;
	config_init(&self.config);

	if ((stream = fmemopen((void *) buf, len, "r")) == NULL) {
		ret = -errno;
		goto out_free;
	}

	ret = config_read(&self.config, stream);
	if (ret == CONFIG_FALSE) {
		fprintf(stderr, "parser: %s (in line %d)\n",
				config_error_text(&self.config),
				config_error_line(&self.config));
		ret = -EINVAL;
		goto out_close;
	}

	INIT_LIST_HEAD(&self.current->task_list);

	ret = parse_script(&self);
	if (ret < 0) {
		ret = -EINVAL;
		cleanup_fail(&self);
		goto out_close;
	}

	cleanup_success(&self);
	*job = self.current;

	/** moved to the job log */
#if 0
	anfs_sched_dump_job(*job, stderr);
#endif

	ret = 0;
	fclose(stream);
	return ret;

out_close:
	fclose(stream);
out_free:
	free(self.current);
	return ret;
}

void anfs_parser_cleanup_job(struct anfs_job *job)
{
	struct anfs_task *t;

	/** FIXME: x2 scanning?? */
	list_for_each_entry(t, &job->task_list, list) {
		if (t)
			cleanup_task_data(t);
	}

	list_for_each_entry(t, &job->task_list, list) {
		if (t)
			cleanup_task(t);
	}
}

