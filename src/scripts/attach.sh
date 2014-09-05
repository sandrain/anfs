#!/bin/sh

#baseip="172.30.248."
#
#if [ $# -gt 0 ]; then # if argument presents, connect via ib
#	baseip="10.37.248."
#fi

## detach any osds if already attached
#./detach.sh || exit 1	## may fail if device is busy

## attach osds from target servers
#targets=(atom-a1 atom-a2 atom-b1 atom-b2 atom-c1 atom-c2 atom-d1 atom-d2)
targets=(atom-a1 atom-a2 atom-b1 atom-b2)
#targets=(atom-a1 atom-a2)
#targets=(atom-a1)

## attach osd targets
#for i in ${serverips[@]}; do
#	ip="$baseip$i"
#	iscsiadm -m discovery -t st -p $ip:3260 --login
#done

for target in ${targets[@]}; do
	iscsiadm -m discovery -t st -p ${target}:3260 --login
done

## display the result
ls /dev | grep ^osd


