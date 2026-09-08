#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP=${1:-lxtmbkafdes01.xarxa.interna:9093}
CFG=${2:-/etc/kafka/admin.properties}

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CFG" --list
