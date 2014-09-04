-- db.schema
--
-- db schema for maintaining namespace, data placement, and workflows of
-- activefs.

--
-- standard unix filesystem features
--

BEGIN TRANSACTION;

-- remove existing ones.
DROP TABLE IF EXISTS anfs_super;
DROP TABLE IF EXISTS anfs_inode;
DROP TABLE IF EXISTS anfs_dirent;
DROP TABLE IF EXISTS anfs_symlink;
DROP TABLE IF EXISTS anfs_object;
DROP TABLE IF EXISTS anfs_job;
DROP TABLE IF EXISTS anfs_tasklet;
DROP TABLE IF EXISTS anfs_tasklet_io;
DROP TABLE IF EXISTS anfs_replica;

-- superblock
CREATE TABLE anfs_super (
	id	INTEGER NOT NULL,
	magic	INTEGER DEFAULT 139810,	-- 0x22222
	version	INTEGER NOT NULL,	-- version #
	ctime	INTEGER NOT NULL,	-- creation time of this version
	ndev	INTEGER NOT NULL,	-- # of devices
	devs	TEXT NOT NULL,		-- device list (comma separated)
	root	INTEGER REFERENCES anfs_inode(id),
	i_submit INTEGER REFERENCES anfs_inode(id),

	PRIMARY KEY (id ASC)
);

-- inodes
CREATE TABLE anfs_inode (
	id	INTEGER NOT NULL,
	dev	INTEGER NOT NULL,
	mode	INTEGER NOT NULL,	-- unix mode
	nlink	INTEGER NOT NULL,	-- # of hard links
	uid	INTEGER NOT NULL,	-- user id
	gid	INTEGER NOT NULL,	-- group id
	rdev	INTEGER NOT NULL,	-- not used for now
	size	INTEGER NOT NULL,	-- file size in bytes
	atime	INTEGER NOT NULL,	-- last access time
	mtime	INTEGER NOT NULL,	-- last modify time
	ctime	INTEGER NOT NULL,	-- creation time
	stloc	INTEGER NOT NULL DEFAULT -1,	-- for arbitrary placement

	PRIMARY KEY (id ASC)
);

-- directories
CREATE TABLE anfs_dirent (
	id	INTEGER NOT NULL,
	d_ino	INTEGER REFERENCES anfs_inode(id), -- directory ino
	e_ino	INTEGER REFERENCES anfs_inode(id), -- entry ino
	name	TEXT,

	PRIMARY KEY (id ASC),
	UNIQUE (d_ino, name)
);

CREATE INDEX idx_e_ino ON anfs_dirent (e_ino);

-- replications
create table anfs_replica (
	id	INTEGER NOT NULL,
	ino	INTEGER REFERENCES anfs_inode(id),
	dev	INTEGER NOT NULL,

	PRIMARY KEY (id ASC)
);

-- trigger for unlinking entries --
CREATE TRIGGER tr_unlink UPDATE OF nlink ON anfs_inode
FOR EACH ROW WHEN NEW.nlink <= 0
BEGIN
	DELETE FROM anfs_dirent WHERE e_ino = NEW.id;
	DELETE FROM anfs_replica WHERE ino = NEW.id;
END;

-- symbolic links
CREATE TABLE anfs_symlink (
	id	INTEGER NOT NULL,
	ino	INTEGER REFERENCES anfs_inode(id),
	path	TEXT,

	PRIMARY KEY (id ASC)
);

--
-- activefs specific features
--

-- active attributes per each file object
CREATE TABLE anfs_object (
	id	INTEGER NOT NULL,
	ino	INTEGER REFERENCES anfs_inode(id),
	trigger	INTEGER	REFERENCES anfs_inode(id), -- for active directories
	origin	INTEGER REFERENCES anfs_tasklet_io(id),

	PRIMARY KEY (id ASC),
	UNIQUE (ino)
);

-- active jobs
CREATE TABLE anfs_job (
	id	INTEGER NOT NULL,
	submit	INTEGER NOT NULL,
	complete INTEGER NOT NULL,
	status	INTEGER NOT NULL,
	script	INTEGER REFERENCES anfs_inode(id),
	name	TEXT,

	PRIMARY KEY (id ASC)
);

-- tasklets
CREATE TABLE anfs_tasklet (
	id	INTEGER NOT NULL,
	job	INTEGER REFERENCES anfs_job(id),	-- to which job this belongs?
	status	INTEGER NOT NULL,	-- status
	submit	INTEGER NOT NULL,	-- submission time
	complete INTEGER NOT NULL,	-- completion time
	dev	INTEGER NOT NULL,	-- device assigned to
	icid	INTEGER NOT NULL,	-- input collection id
	ocid	INTEGER NOT NULL,	-- output collection id
	name	TEXT,

	primary key (id ASC)
);

-- tasklet input/output
CREATE TABLE anfs_tasklet_io (
	id	INTEGER NOT NULL,
	type	INTEGER NOT NULL,	-- 0 for input, 1 for output
	tasklet	INTEGER REFERENCES anfs_tasklet(id),
	data	INTEGER REFERENCES anfs_inode(id),

	PRIMARY KEY (id ASC),
	UNIQUE (tasklet, type, data)
);

-- hardcode special inodes here (root inode, .submit: job submit file)
-- inode: 1
insert into anfs_inode
	(dev, mode, nlink, uid, gid, rdev, size, atime, mtime, ctime)
values
	(0, 16832, 2, 0, 0, 0, 4096,
	strftime('%s', 'now'), strftime('%s', 'now'), strftime('%s', 'now'));

insert into anfs_dirent (d_ino, e_ino, name)
	values (1, 1, '.');
insert into anfs_dirent (d_ino, e_ino, name)
	values (1, 1, '..');

END TRANSACTION;

