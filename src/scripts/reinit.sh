#!/bin/bash

umount ../mnt

./detach.sh 
./serverdown.sh
./pathdb-init.sh
./serverup.sh 1
./attach.sh

cd .. && ./mkfs.sh
