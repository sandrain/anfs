#!/bin/bash

pathdb="/ccs/techint/home/hs2/afs_eval/pathdb.db"

rm -f $pathdb
sqlite3 $pathdb < ../pathdb.schema.sql

hosts=`iscsiadm -m session | awk '{print $NF}' | cut -d'.' -f1`

i=0
for host in $hosts; do
	sql="insert into anfs_hostname (host, osd) values "
	sql+="('$host', $i)"
	i=$((i+1))

	sqlite3 $pathdb "$sql"
done

