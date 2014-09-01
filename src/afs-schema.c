const char *create_sql =
"-- db.schema\n"
"--\n"
"-- db schema for maintaining namespace, data placement, and workflows of\n"
"-- activefs.\n"
"\n"
"--\n"
"-- standard unix filesystem features\n"
"--\n"
"\n"
"BEGIN TRANSACTION;\n"
"\n"
"-- remove existing ones.\n"
"DROP TABLE IF EXISTS afs_super;\n"
"DROP TABLE IF EXISTS afs_inode;\n"
"DROP TABLE IF EXISTS afs_dirent;\n"
"DROP TABLE IF EXISTS afs_dirent_unlinked;\n"
"DROP TABLE IF EXISTS afs_symlink;\n"
"DROP TABLE IF EXISTS afs_object;\n"
"DROP TABLE IF EXISTS afs_job;\n"
"DROP TABLE IF EXISTS afs_tasklet;\n"
"DROP TABLE IF EXISTS afs_tasklet_io;\n"
"DROP TABLE IF EXISTS afs_replica;\n"
"\n"
"-- superblock\n"
"CREATE TABLE afs_super (\n"
"	id	INTEGER NOT NULL,\n"
"	magic	INTEGER DEFAULT 139810,	-- 0x22222\n"
"	version	INTEGER NOT NULL,	-- version #\n"
"	ctime	INTEGER NOT NULL,	-- creation time of this version\n"
"	ndev	INTEGER NOT NULL,	-- # of devices\n"
"	devs	TEXT NOT NULL,		-- device list (comma separated)\n"
"	stsize	INTEGER NOT NULL,	-- default stripe size in bytes\n"
"	stwidth	INTEGER NOT NULL DEFAULT 0,	-- stripe width\n"
"						-- (default: using all devs)\n"
"	direct	INTEGER NOT NULL DEFAULT 0,	-- use direct osd?\n"
"						-- (default: using exofs)\n"
"	root	INTEGER REFERENCES afs_inode(id),\n"
"	i_jobs	INTEGER REFERENCES afs_inode(id), -- special files\n"
"	i_failed INTEGER REFERENCES afs_inode(id),\n"
"	i_running INTEGER REFERENCES afs_inode(id),\n"
"	i_submit INTEGER REFERENCES afs_inode(id),\n"
"\n"
"	PRIMARY KEY (id ASC)\n"
");\n"
"\n"
"-- inodes\n"
"CREATE TABLE afs_inode (\n"
"	id	INTEGER NOT NULL,\n"
"	mode	INTEGER NOT NULL,	-- unix mode\n"
"	nlink	INTEGER NOT NULL,	-- # of hard links\n"
"	uid	INTEGER NOT NULL,	-- user id\n"
"	gid	INTEGER NOT NULL,	-- group id\n"
"	rdev	INTEGER NOT NULL,	-- not used for now\n"
"	size	INTEGER NOT NULL,	-- file size in bytes\n"
"	ctime	INTEGER NOT NULL,	-- creation time\n"
"	atime	INTEGER NOT NULL,	-- last access time\n"
"	mtime	INTEGER NOT NULL,	-- last modify time\n"
"	stmode	INTEGER NOT NULL DEFAULT 0,	-- stripe mode\n"
"	stsize	INTEGER NOT NULL DEFAULT 0,	-- stripe size (immutable)\n"
"	stwidth	INTEGER NOT NULL DEFAULT 0,	-- stripe width (immutable)\n"
"	stloc	INTEGER NOT NULL DEFAULT -1,	-- for arbitrary placement\n"
"	virtual	INTEGER NOT NULL DEFAULT 0,	-- is this virtual?\n"
"\n"
"	PRIMARY KEY (id ASC)\n"
");\n"
"\n"
"-- directories\n"
"CREATE TABLE afs_dirent (\n"
"	id	INTEGER NOT NULL,\n"
"	d_ino	INTEGER REFERENCES afs_inode(id), -- directory ino\n"
"	e_ino	INTEGER REFERENCES afs_inode(id), -- entry ino\n"
"	virtual	INTEGER NOT NULL DEFAULT 0,	-- virtual entry?\n"
"	name	TEXT,\n"
"\n"
"	PRIMARY KEY (id ASC),\n"
"	UNIQUE (d_ino, name)\n"
");\n"
"\n"
"CREATE INDEX idx_e_ino ON afs_dirent (e_ino);\n"
"\n"
"-- removed entires\n"
"create table afs_dirent_unlinked (\n"
"	id	INTEGER NOT NULL,\n"
"	time	INTEGER DEFAULT (strftime('%s', 'now')),\n"
"	d_ino	INTEGER REFERENCES afs_inode(id), -- directory ino\n"
"	e_ino	INTEGER REFERENCES afs_inode(id), -- entry ino\n"
"	name	TEXT,\n"
"\n"
"	PRIMARY KEY (id ASC)\n"
");\n"
"\n"
"-- replications\n"
"create table afs_replica (\n"
"	id	INTEGER NOT NULL,\n"
"	ino	INTEGER REFERENCES afs_inode(id),\n"
"	dev	INTEGER NOT NULL,\n"
"\n"
"	PRIMARY KEY (id ASC)\n"
");\n"
"\n"
"-- trigger for unlinking entries --\n"
"CREATE TRIGGER tr_unlink UPDATE OF nlink ON afs_inode\n"
"FOR EACH ROW WHEN NEW.nlink <= 0\n"
"BEGIN\n"
"INSERT INTO afs_dirent_unlinked (d_ino, e_ino, name) \n"
"  SELECT d_ino, e_ino, name FROM afs_dirent WHERE e_ino = NEW.id;\n"
"DELETE FROM afs_dirent WHERE e_ino = NEW.id;\n"
"DELETE FROM afs_replica WHERE ino = NEW.id;\n"
"END;\n"
"\n"
"-- symbolic links\n"
"CREATE TABLE afs_symlink (\n"
"	id	INTEGER NOT NULL,\n"
"	ino	INTEGER REFERENCES afs_inode(id),\n"
"	path	TEXT,\n"
"\n"
"	PRIMARY KEY (id ASC)\n"
");\n"
"\n"
"--\n"
"-- activefs specific features\n"
"--\n"
"\n"
"-- active attributes per each file object\n"
"CREATE TABLE afs_object (\n"
"	id	INTEGER NOT NULL,\n"
"	ino	INTEGER REFERENCES afs_inode(id),\n"
"	trigger	INTEGER	REFERENCES afs_inode(id), -- for active directories\n"
"	origin	INTEGER REFERENCES afs_tasklet_io(id),\n"
"\n"
"	PRIMARY KEY (id ASC),\n"
"	UNIQUE (ino)\n"
");\n"
"\n"
"-- active jobs\n"
"CREATE TABLE afs_job (\n"
"	id	INTEGER NOT NULL,\n"
"	submit	INTEGER NOT NULL,\n"
"	complete INTEGER NOT NULL,\n"
"	status	INTEGER NOT NULL,\n"
"	script	INTEGER REFERENCES afs_inode(id),\n"
"	name	TEXT,\n"
"\n"
"	PRIMARY KEY (id ASC)\n"
");\n"
"\n"
"-- tasklets\n"
"CREATE TABLE afs_tasklet (\n"
"	id	INTEGER NOT NULL,\n"
"	job	INTEGER REFERENCES afs_job(id),	-- to which job this belongs?\n"
"	status	INTEGER NOT NULL,	-- status\n"
"	submit	INTEGER NOT NULL,	-- submission time\n"
"	complete INTEGER NOT NULL,	-- completion time\n"
"	dev	INTEGER NOT NULL,	-- device assigned to\n"
"	icid	INTEGER NOT NULL,	-- input collection id\n"
"	ocid	INTEGER NOT NULL,	-- output collection id\n"
"	name	TEXT,\n"
"\n"
"	primary key (id ASC)\n"
");\n"
"\n"
"-- tasklet input/output\n"
"CREATE TABLE afs_tasklet_io (\n"
"	id	INTEGER NOT NULL,\n"
"	type	INTEGER NOT NULL,	-- 0 for input, 1 for output\n"
"	tasklet	INTEGER REFERENCES afs_tasklet(id),\n"
"	data	INTEGER REFERENCES afs_inode(id),\n"
"\n"
"	PRIMARY KEY (id ASC),\n"
"	UNIQUE (tasklet, type, data)\n"
");\n"
"\n"
"END TRANSACTION;\n"
"\n"
;
