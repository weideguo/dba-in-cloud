#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

base_dir=$(cd "$(dirname "${script_dir}")" &>/dev/null && pwd -P)

#echo $script_dir
#echo $base_dir
python3 bin/mysql_binlog_backup.py >> logs/mysql_binlog_backup.log 2>>logs/mysql_binlog_backup.err &
sleep 3
ps -ef | grep mysql_binlog_backup
