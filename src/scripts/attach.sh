#!/bin/sh

servers=(`echo $targets | tr "," "\n"`)

for target in ${servers[@]}; do
	iscsiadm -m discovery -t st -p ${target}:3260 --login
done

## display the result
ls /dev | grep ^osd


