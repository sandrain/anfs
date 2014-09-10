#!/bin/bash

rm -f /tmp/anfs.db

rm -rf /mnt/afe0/* /mnt/afe1/*
./mkfs.anfs -d /mnt/afe0 -d /mnt/afe1 -d /mnt/afe2 -d /mnt/afe3

if [ -d "/tmp/afsjobs" ]; then
	rm -f /tmp/afsjobs/*
else
	mkdir /tmp/afsjobs
fi

