/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * some utility functions.
 */
#ifndef	__ANFS_UTIL_H__
#define	__ANFS_UTIL_H__

/**
 * commonly used stuffs..
 */

#ifndef	anfs_max
#define anfs_max(x, y)	((x) > (y) ? (x) : (y))
#endif

#ifndef	anfs_min
#define	anfs_min(x, y)	((x) < (y) ? (x) : (y))
#endif

#ifndef	anfs_llu
#define anfs_llu(x)		((unsigned long long) (x))
#endif

#define	__anfs_unused(x)		((void) (x))

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/**
 * linked list implementation taken from linux
 */
#include "list.h"

#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>

/**
 * error handling
 */

static inline void anfs_err_warn(const char *s)
{
	fprintf(stderr, "%s\n", s);
}

static inline void anfs_err_abort(const char *s)
{
	fprintf(stderr, "%s\n", s);
	abort();
}

/**
 * dynamic memory allocation wrappers:
 * there is no way to recover, except for abnormal termination.
 */

static inline char *anfs_strdup(const char *s)
{
	char *ret = strdup(s);
	if (!ret)
		anfs_err_abort("memory allocation failed (strdup)");
	return ret;
}

static inline void *anfs_malloc(size_t size)
{
	void *ret = malloc(size);
	if (!ret)
		anfs_err_abort("memory allocation failed (malloc)");
	return ret;
}

static inline void *anfs_calloc(size_t nmemb, size_t size)
{
	void *ret = calloc(nmemb, size);
	if (!ret)
		anfs_err_abort("memory allocation failed (calloc)");
	return ret;
}

/**
 * strtrim implementation.
 *
 * note that for trimming operation (especially for ltrim), the caller lose the
 * pointer to the original string.
 * also be warned that this was not tested fully.
 *
 * taken from: http://stackoverflow.com/questions/656542/trim-a-string-in-c
 * written by JRL
 */

static inline char *strrtrim(char *s)
{
	while (isspace(*s)) s++;
	return s;
}

static inline char *strltrim(char *s)
{
	char *back = &s[strlen(s)];
	while (isspace(*--back));
	back[1] = '\0';
	return s;
}

static inline char *strtrim(char *str)
{
	return strrtrim(strltrim(str));
}

static inline int strempty(char *str)
{
	if (strlen(str) == 0)
		return 1;

	while (*str)
		if (!isspace(*str++))
			return 0;
	return 1;
}

/**
 * getting timestamp of current time
 */

static inline uint64_t anfs_now_sec(void)
{
	struct timeval tmp;
	gettimeofday(&tmp, NULL);
	return tmp.tv_sec;
}

#define anfs_now	anfs_now_sec

static inline uint64_t anfs_now_usec(void)
{
	struct timeval tmp;
	gettimeofday(&tmp, NULL);
	return tmp.tv_sec * 1000000 + tmp.tv_usec;
}

/**
 * get inode number from path
 */
static inline uint64_t get_file_ino(const char *path)
{
	struct stat buf;
	int ret = stat(path, &buf);

	if (ret < 0)
		return 0;

	return buf.st_ino;
}

/**
 * simple hash table wrapper using std hsearch.
 */
#include <string.h>
#include <search.h>

typedef	struct hsearch_data	anfs_htable;

static inline anfs_htable *anfs_hash_init(size_t nel, anfs_htable *htab)
{
	memset(htab, 0, sizeof(*htab));

	return hcreate_r(nel, htab) ? htab : NULL;
}

static inline void anfs_hash_exit(anfs_htable *htab)
{
	hdestroy_r(htab);
}

static inline int anfs_hash_insert(anfs_htable *htab, const char *key,
					void *val)
{
	ENTRY e, *tmp;

	e.key = (char *) key;
	e.data = val;

	return hsearch_r(e, ENTER, &tmp, htab) ? 0 : errno;
}

static inline void *anfs_hash_search(anfs_htable *htab, const char *key)
{
	ENTRY e, *tmp;

	e.key = (char *) key;
	e.data = NULL;

	return hsearch_r(e, FIND, &tmp, htab) ? tmp->data : NULL;
}

#endif	/** __ANFS_UTIL_H__ */

