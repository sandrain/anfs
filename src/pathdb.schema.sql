--
-- pathdb.schema.sql
--

-- we only need one table to keep track of obj to path mapping.

BEGIN TRANSACTION;

DROP TABLE IF EXISTS anfs_obj_nspath;

CREATE TABLE anfs_nspath (
	id	INTEGER NOT NULL,
	pid	INTEGER NOT NULL,
	oid	INTEGER NOT NULL,
	nspath	TEXT NOT NULL,
	runtime	INTEGER NOT NULL DEFAULT 0,

	PRIMARY KEY (id asc),
	UNIQUE (pid, oid)
);

CREATE INDEX idx_nspath ON anfs_nspath (nspath);

END TRANSACTION;

