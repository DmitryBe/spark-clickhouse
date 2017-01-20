#!/bin/bash

service clickhouse-server start
#exec "$@"
tail -f /var/log/clickhouse-server/*