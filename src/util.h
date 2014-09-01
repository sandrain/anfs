/* Copyright (C) 2013	 - Hyogi Sim <hyogi@cs.vt.edu>
 * 
 * Please refer to COPYING for the license.
 * ---------------------------------------------------------------------------
 * some utility functions.
 */
#ifndef	__AFS_UTIL_H__
#define	__AFS_UTIL_H__

/**
 * commonly used stuffs..
 */

#ifndef	afs_max
#define afs_max(x, y)	((x) > (y) ? (x) : (y))
#endif

#ifndef	afs_min
#define	afs_min(x, y)	((x) < (y) ? (x) : (y))
#endif

#ifndef	afs_llu
#define afs_llu(x)		((unsigned long long) (x))
#endif

#define	__afs_unused(x)		((void) (x))

#include <sys/time.h>

static inline uint64_t afs_now(void)
{
	struct timeval t;
	gettimeofday(&t, NULL);
	return t.tv_sec;
}

/**
 * linked list implementation taken from linux
 */
#include "list.h"

#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>

/**
 * error handling
 */

static inline void afs_err_warn(const char *s)
{
	fprintf(stderr, "%s\n", s);
}

static inline void afs_err_abort(const char *s)
{
	fprintf(stderr, "%s\n", s);
	abort();
}

/**
 * dynamic memory allocation wrappers:
 * there is no way to recover, except for abnormal termination.
 */

static inline char *afs_strdup(const char *s)
{
	char *ret = strdup(s);
	if (!ret)
		afs_err_abort("memory allocation failed (strdup)");
	return ret;
}

static inline void *afs_malloc(size_t size)
{
	void *ret = malloc(size);
	if (!ret)
		afs_err_abort("memory allocation failed (malloc)");
	return ret;
}

static inline void *afs_calloc(size_t nmemb, size_t size)
{
	void *ret = calloc(nmemb, size);
	if (!ret)
		afs_err_abort("memory allocation failed (calloc)");
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
 * simple hash table wrapper using std hsearch.
 */
#include <string.h>
#include <search.h>

typedef	struct hsearch_data	afs_htable;

static inline afs_htable *afs_hash_init(size_t nel, afs_htable *htab)
{
	memset(htab, 0, sizeof(*htab));

	return hcreate_r(nel, htab) ? htab : NULL;
}

static inline void afs_hash_exit(afs_htable *htab)
{
	hdestroy_r(htab);
}

static inline int afs_hash_insert(afs_htable *htab, const char *key, void *val)
{
	ENTRY e, *tmp;

	e.key = (char *) key;
	e.data = val;

	return hsearch_r(e, ENTER, &tmp, htab) ? 0 : errno;
}

static inline void *afs_hash_search(afs_htable *htab, const char *key)
{
	ENTRY e, *tmp;

	e.key = (char *) key;
	e.data = NULL;

	return hsearch_r(e, FIND, &tmp, htab) ? tmp->data : NULL;
}

#endif	/** __AFS_UTIL_H__ */

