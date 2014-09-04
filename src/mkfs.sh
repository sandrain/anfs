#!/bin/bash

rm -f /tmp/anfs.db

rm -rf /mnt/afe0/* /mnt/afe1/*
./mkfs.anfs -d /mnt/afe0 -d /mnt/afe1

if [ -d "/tmp/afsjobs" ]; then
	rm -f /tmp/afsjobs/*
else
	mkdir /tmp/afsjobs
fi

cd scripts
./pathdb-init.sh

