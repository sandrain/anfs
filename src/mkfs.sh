#!/bin/bash

rm -f /tmp/afs.db
#./mkactivefs -r -d /dev/osd0 -d /dev/osd1 -d /dev/osd2 -d /dev/osd3 \
#                -d /dev/osd4 -d /dev/osd5 -d /dev/osd6 -d /dev/osd7

#./mkactivefs -r -d /dev/osd0 -d /dev/osd1 -d /dev/osd2 -d /dev/osd3
#./mkactivefs -r -d /dev/osd0 -d /dev/osd1
#./mkactivefs -r -d /dev/osd0

./mkactivefs -d /mnt/afe0 -d /mnt/afe1

if [ -d "/tmp/afsjobs" ]; then
	rm -f /tmp/afsjobs/*
else
	mkdir /tmp/afsjobs
fi

