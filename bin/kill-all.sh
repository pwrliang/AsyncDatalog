#!/usr/bin/env bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    ssh -n ${USER}@${line} "kill -9 \$(ps aux|grep '[s]ocialite.master='|awk '{print \$2}') 2> /dev/null"
done < "$1"