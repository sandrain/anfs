#
# Makefile
#

CC = gcc
LD = $(CC)
COPTS = -ggdb -O0
#COPTS = -O2
#COPTS += -pg
COPTS += -fPIC -foptimize-sibling-calls -D_GNU_SOURCE -D_BSD_SOURCE
CWARN = -Wall -Werror -Wpointer-arith -Wwrite-strings -Wcast-align -Wundef \
	 -Wbad-function-cast -Wundef -Wmissing-prototypes -Wnested-externs \
	 -Wmissing-declarations -Wno-unused
CFLAGS = $(COPTS) $(CWARN)

OSDPATH = ../../activeosd/open-osd
CFLAGS += -I$(OSDPATH)/include -I$(OSDPATH)/include/open-osd -I$(OSDPATH)/lib
CFLAGS += $(shell pkg-config --cflags fuse)
CFLAGS += $(shell pkg-config --cflags sqlite3)
CFLAGS += $(shell pkg-config --cflags libconfig)

SRC := activefs.c config.c filer.c db.c osd.c sched.c util.c virtio.c \
	parser.c lineage.c pathdb-host.c
INC := activefs.h activefs-priv.h config.h db.h filer.h list.h osd.h sched.h \
	util.h virtio.h parser.h lineage.h pathdb-host.h
OBJ := $(SRC:.c=.o)
LIB := $(shell pkg-config --libs sqlite3)
LIB += $(shell pkg-config --libs fuse)
LIB += $(shell pkg-config --libs libconfig)
LIB += -L../../activeosd/open-osd/lib -losd

DEPEND = .dependencies
TARGETS = mkactivefs activefs
PATHDB = /ccs/techint/home/hs2/afs_eval/pathdb.db

all: $(DEPEND) $(TARGETS)

#pathdb: $(PATHDB)


## activefs
activefs: $(OBJ)
	$(CC) -o $@ $^ $(LIB) $(CFLAGS)

%.o: %.c Makefile $(INC)
	$(CC) $(CFLAGS) -c $< -o $@ $(@:.o=.c)

$(DEPEND): $(OBJ:.o=.c)
	$(CC) -MM $(CFLAGS) $^ > $@

## mkactivefs
mkactivefs: mkactivefs.o afs-schema.o
	$(CC) $(CFLAGS) $(shell pkg-config --cflags --libs sqlite3) \
		$(LIB) -o $@ $^

mkactivefs.o: mkactivefs.c
	$(CC) $(CFLAGS) -c $< -o $@

afs-schema.o: afs-schema.c
	$(CC) $(CFLAGS) -c $< -o $@

afs-schema.c: db.schema.sql
	@( echo "const char *create_sql =";\
	   sed 's/^/"/; s/$$/\\n"/' < $< ;\
	   echo ";" ) > $@

clean:
	rm -f *.o $(DEPEND) $(TARGETS) afs-schema.c

## pathdb.db
$(PATHDB): pathdb.schema.sql
	rm -f $(PATHDB)
	sqlite3 $(PATHDB) < pathdb.schema.sql

