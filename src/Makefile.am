#
# Copyright © 2014	UT-Battelle, LLC.
#			All rights reserved.
# $COPYRIGHT
#

bin_PROGRAMS = 		\
	anfs		\
	mkfs.anfs

pkgdata_DATA =		\
	db.schema.sql

EXTRA_DIST =		\
	db.schema.sql

anfs_SOURCES = 		\
	anfs.c 		\
	anfs.h		\
	anfs_config.c	\
	anfs_config.h	\
	anfs_mdb.c	\
	anfs_mdb.h	\
	anfs_osd.c	\
	anfs_osd.h	\
	anfs_lineage.c	\
	anfs_lineage.h	\
	anfs_parser.c	\
	anfs_parser.h	\
	anfs_store.c	\
	anfs_store.h	\
	anfs_sched.h	\
	anfs_sched.c
anfs_LDADD = $(FUSE_LIBS) $(SQLITE3_LIBS) $(LIBCONFIG_LIBS)
anfs_CPPFLAGS = $(FUSE_CFLAGS) $(SQLITE3_CFLAGS) $(LIBCONFIG_CFLAGS)
anfs_LDFLAGS = $(FUSE_LIBS) $(SQLITE3_LIBS) $(LIBCONFIG_LIBS)

mkanfs.o: mkanfs.c
	$(CC) $(CPPFLAGS) $(FUSE_CFLAGS) $(SQLITE3_CFLAGS) $(LIBCONFIG_CFLAGS) -c $< -o $@ $(FUSE_LIBS) $(SQLITE3_LIBS) $(LIBCONFIG_LIBS)
.PHONY: mkanfs.o

anfs-schema.c: db.schema.sql
	@( echo "const char *create_sql =";\
	   sed 's/^/"/; s/$$/\\n"/' < $< ;\
	   echo ";" ) > $@
anfs-schema.o: anfs-schema.c
	$(CC) $(CFLAGS) -c $< -o $@
.PHONY: anfs-schema.o

mkfs_anfs_SOURCES =
mkfs_anfs_LDADD = mkanfs.o anfs-schema.o $(FUSE_LIBS) $(SQLITE3_LIBS) $(LIBCONFIG_LIBS)
