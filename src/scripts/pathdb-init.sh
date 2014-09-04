#!/bin/bash

pathdb="/ccs/techint/home/hs2/afs_eval/pathdb.db"

rm -f $pathdb
sqlite3 $pathdb < ../pathdb.schema.sql

