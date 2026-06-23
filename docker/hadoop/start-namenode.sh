#!/usr/bin/env bash
set -euo pipefail

name_dir=/hadoop/dfs/name

if [[ ! -d "${name_dir}/current" ]]; then
  hdfs namenode -format -nonInteractive
fi

exec hdfs namenode
