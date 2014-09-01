/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * parse the job script and build data structures for scheduler.
 */
#include <errno.h>
#include <libconfig.h>
#include "activefs.h"

struct afs_parser_data {
	config_t config;
	struct afs_job *current;
	const char *script;
	const char *bindir;
	const char *datadir;
	afs_htable datahash;
};

static inline void cleanup_task(struct afs_task *t)
{
	if (t->name && t->name != t->kernel)
		free((void *) t->name);
	if (t->kernel)
		free((void *) t->kernel);
	if (t->argument)
		free((void *) t->argument);
	free(t);
}

static inline void cleanup_task_data(struct afs_task *t)
{
	int i;
	struct afs_task_data *td;
	struct afs_data_file *df;

	if (t->input) {
		td = t->input;
#if 0
		for (i = 0; i < td->n_files; i++) {
			df = td->files[i];
			if (df) {
				free(df);
				df = NULL;
			}

		}
#endif
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

static inline void cleanup_success(struct afs_parser_data *self)
{
	afs_hash_exit(&self->datahash);
	config_destroy(&self->config);
}

static inline void cleanup_fail(struct afs_parser_data *self)
{
	if (self->current)
		afs_parser_cleanup_job(self->current);

	cleanup_success(self);
}

static const char *get_afs_bin_path(struct afs_parser_data *self, const char *s)
{
	char *sb;

	if (s[0] == '/')
		return afs_strdup(s);

	sb = afs_calloc(1, strlen(self->bindir) + strlen(s) + 1);
	sprintf(sb, "%s%s", self->bindir, s);

	return sb;
}

static const char *get_afs_data_path(struct afs_parser_data *self,
					const char *s)
{
	char *sb;

	if (s[0] == '/')
		return afs_strdup(s);

	sb = afs_calloc(1, strlen(self->datadir) + strlen(s) + 1);
	sprintf(sb, "%s%s", self->datadir, s);

	return sb;
}

static struct afs_task *alloc_tasklet(struct afs_parser_data *self,
				struct afs_job *job, config_setting_t *task)
{
	int ret = 0;
	long affinity = -1;
	const char *tmp;
	struct afs_task *tasklet = afs_calloc(1, sizeof(*tasklet));

	tasklet->job = job;

	ret = config_setting_lookup_string(task, "kernel", &tmp);
	if (ret != CONFIG_TRUE)
		goto out_free;
	tasklet->kernel = get_afs_bin_path(self, tmp);

	ret = config_setting_lookup_string(task, "name", &tmp);
	if (ret != CONFIG_TRUE)
		tasklet->name = tasklet->kernel;
	else
		tasklet->name = afs_strdup(tmp);

	ret = config_setting_lookup_string(task, "argument", &tmp);
	if (ret == CONFIG_TRUE)
		tasklet->argument = afs_strdup(tmp);

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

static int alloc_tasklet_input(struct afs_parser_data *self,
			config_setting_t *task, struct afs_task *tasklet)
{
	int i;
	int count;
	const char *tmp;
	struct afs_data_file **file, *dt;
	config_setting_t *input = config_setting_get_member(task, "input");

	if (!input) {
		/**
		 * the task doesn't have any input.
		 */
		tasklet->input = afs_calloc(1, sizeof(struct afs_task_data));
		tasklet->input->n_files = 0;
		return 0;
	}

	count = config_setting_length(input);
	tasklet->input = afs_calloc(1, sizeof(struct afs_task_data) +
					count * sizeof(struct afs_data_file *));
	tasklet->input->n_files = count;

	for (i = 0; i < count; i++) {
		tmp = config_setting_get_string_elem(input, i);
		if (!tmp)
			return -2;

		tmp = get_afs_data_path(self, tmp);
		dt = afs_hash_search(&self->datahash, tmp);

		if (dt)
			tasklet->input->files[i] = dt;
		else {
			file = &tasklet->input->files[i];
			*file = afs_calloc(1, sizeof(struct afs_data_file));
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
static int alloc_tasklet_output(struct afs_parser_data *self,
			config_setting_t *task, struct afs_task *tasklet)
{
	int i;
	int count;
	const char *tmp;
	struct afs_data_file **file;
	config_setting_t *output = config_setting_get_member(task, "output");

	if (!output)
		return -1;

	count = config_setting_length(output);

	tasklet->output = afs_calloc(1, sizeof(struct afs_task_data) +
				count * sizeof(struct afs_data_file *));
	tasklet->output->n_files = count;

	for (i = 0; i < count; i++) {
		file = &tasklet->output->files[i];
		tmp = config_setting_get_string_elem(output, i);
		tmp = get_afs_data_path(self, tmp);

		if (!tmp)
			return -2;

		(*file) = afs_calloc(1, sizeof(struct afs_data_file));
		(*file)->path = tmp;
		(*file)->producer = tasklet;

		afs_hash_insert(&self->datahash, tmp, *file);
	}

	return 0;
}

static inline int alloc_tasklet_ios(struct afs_parser_data *self, 
			config_setting_t *task, struct afs_task *tasklet)
{
	int ret;

	ret = alloc_tasklet_input(self, task, tasklet);
	if (ret)
		return ret;

	ret = alloc_tasklet_output(self, task, tasklet);
	return ret;
}

#if 0
static int process_argument(struct afs_parser_data *self, struct afs_task *task)
{
	return 0;
}
#endif

static int build_tasks(struct afs_parser_data *self, struct afs_job *job,
			config_setting_t *task, struct list_head **link)
{
	int ret;
	struct afs_task *tasklet;

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

static int parse_script(struct afs_parser_data *self)
{
	int i, ret, count, sched = 0;
	int n_tasks = 0;
	const char *tmp;
	struct list_head *link;
	config_setting_t *tasks;
	config_t *config = &self->config;
	struct afs_job *job = self->current;

	if (config_lookup_string(config, "name", &tmp))
		job->name = afs_strdup(tmp);
	if (config_lookup_string(config, "bindir", &tmp))
		self->bindir = tmp;
	if (config_lookup_string(config, "datadir", &tmp))
		self->datadir = tmp;
	if (config_lookup_string(config, "schedule", &tmp)) {
		if (strncmp(tmp, "rr", strlen("rr")) == 0)
			sched = AFS_SCHED_POLICY_RR;
		else if (strncmp(tmp, "input", strlen("input")) == 0)
			sched = AFS_SCHED_POLICY_INPUT;
		else if (strncmp(tmp, "minwait", strlen("minwait")) == 0)
			sched = AFS_SCHED_POLICY_MINWAIT;
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

int afs_parser_parse_script(const char *buf, size_t len, struct afs_job **job)
{
	int ret;
	FILE *stream;
	struct afs_parser_data self;

	self.current = afs_calloc(1, sizeof(*self.current));
	if (afs_hash_init(100, &self.datahash) == NULL)
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
	afs_sched_dump_job(*job, stderr);
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

void afs_parser_cleanup_job(struct afs_job *job)
{
	struct afs_task *t;

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


/** XXX: we don't spawn a separated thread for now. */
#if 0
#include <pthread.h>
static inline
struct afs_parser_data *fetch_work(struct list_head *q, pthread_mutex_t *lock)
{
	struct afs_parser_data *work = NULL;

	pthread_mutex_lock(lock);
	if (list_empty(q))
		goto out;

	work = list_first_entry(q, struct afs_parser_data, list);
	list_del(&work->list);

out:
	pthread_mutex_unlock(lock);
	return work;
}

static void *parser_worker_func(void *arg)
{
	int ret;
	unsigned long count = 0;
	struct afs_parser *parser = (struct afs_parser *) arg;
	pthread_mutex_t *lock = &parser->wq_lock;
	struct list_head *q = &parser->wq;
	struct afs_parser_data *work;

	while (1) {
		work = fetch_work(q, lock);
		if (!work) {
			usleep(500);
			continue;
		}

		/** process the parsing.. */
		ret = __parse_job_script(work);

		pthread_cond_signal(&work->cond);
	}

	return (void *) count;
}

int afs_parser_init(struct afs_parser *self, struct afs_ctx *ctx)
{
	pthread_t t;

	self->ctx = ctx;

	pthread_mutex_init(&self->wq_lock, NULL);
	INIT_LIST_HEAD(&self->wq);

	t = pthread_create(&t, NULL, &parser_worker_func, self);
	if (t) {
		pthread_mutex_destroy(&self->wq_lock);
		return errno;
	}

	self->worker = t;

	return 0;
}

void afs_parser_exit(struct afs_parser *self)
{
	void *ret;

	pthread_mutex_destroy(&self->wq_lock);
	(void) pthread_cancel(self->worker);
	(void) pthread_join(self->worker, &ret);
}

int afs_parser_parse_script(struct afs_parser *self, const char *buf,
				size_t len, struct afs_job **job)
{
	int ret;
	struct afs_parser_data work;

	work.parser = self;
	work.buf = buf;
	work.len = len;
	work.job = NULL;
	pthread_cond_init(&work.cond, NULL);
	pthread_mutex_init(&work.lock, NULL);

	pthread_mutex_lock(&self->wq_lock);
	list_add_tail(&work.list, &self->wq);
	pthread_mutex_unlock(&self->wq_lock);

	pthread_mutex_lock(&work.lock);
	ret = pthread_cond_wait(&work.cond, &work.lock);
	pthread_mutex_unlock(&work.lock);

	if (!work.job)	/** parse failed */
		ret = -1;
	else {
		ret = 0;
		*job = work.job;
	}

	return ret;
}
#endif

