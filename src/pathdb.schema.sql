--
-- pathdb.schema.sql
--

-- we only need one table to keep track of obj to path mapping.

BEGIN TRANSACTION;

DROP TABLE IF EXISTS anfs_nspath;
DROP TABLE IF EXISTS anfs_hostname;
DROP TABLE IF EXISTS anfs_oids;

CREATE TABLE anfs_nspath (
	id	INTEGER NOT NULL,
	ino	INTEGER NOT NULL,
	nspath	TEXT NOT NULL,
	runtime	INTEGER NOT NULL DEFAULT 0,

	PRIMARY KEY (id asc),
	unique (ino),
	unique (nspath)
);

CREATE INDEX idx_nspath ON anfs_nspath (nspath);

CREATE TABLE anfs_hostname (
	id	INTEGER NOT NULL,
	host	TEXT NOT NULL,
	osd	INTEGER NOT NULL,

	PRIMARY KERY (id ASC),
	UNIQUE (host, osd)
);

CREATE TABLE anfs_oids (
	id	INTEGER NOT NULL,
	ino	INTEGER NOT NULL REFERENCES anfs_nspath(ino),
	osd	INTEGER NOT NULL,
	oid	INTEGER NOT NULL,

	PRIMARY KEY (id ASC),
	UNIQUE (ino, osd, oid)
);

END TRANSACTION;

