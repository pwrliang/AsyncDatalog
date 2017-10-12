#!/usr/bin/env bash

SOCIALITE_PREFIX=/home/gengl/socialite-before-yarn
tar -zcf /tmp/socialite.tar.gz -C ${SOCIALITE_PREFIX}/../ socialite-before-yarn
while IFS='' read -r line || [[ -n "$line" ]]; do
    scp /tmp/socialite.tar.gz gengl@${line}:/tmp/
    ssh gengl@${line} "rm -rf ${SOCIALITE_PREFIX} 2> /dev/null && tar -zxf /tmp/socialite.tar.gz -C /home/gengl/"
done < "$1"