#!/usr/bin/env bash
host_name=$1
dir=~/socialite-before-yarn
tar -zcf $dir/out.tar.gz -C $dir out conf examples
scp $dir/out.tar.gz gengl@$host_name:"/home/gengl/socialite-before-yarn/"
ssh gengl@$host_name "rm -rf $dir/out 2> /dev/null && tar -zxf $dir/out.tar.gz -C $dir/ && rm $dir/out.tar.gz"
rm out.tar.gz
