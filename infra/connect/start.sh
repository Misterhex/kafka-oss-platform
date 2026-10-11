#!/usr/bin/env bash
set -euo pipefail
mkdir -p /tmp/connect-plugins
cat >/tmp/worker.properties <<EOF
bootstrap.servers=${BOOTSTRAP}
group.id=connect-${CLUSTER}
listeners=http://0.0.0.0:8083
rest.advertised.host.name=connect-${CLUSTER}
rest.advertised.port=8083
config.storage.topic=_connect-${CLUSTER}-configs
offset.storage.topic=_connect-${CLUSTER}-offsets
status.storage.topic=_connect-${CLUSTER}-status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
offset.storage.partitions=1
status.storage.partitions=1
offset.flush.interval.ms=1000
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
plugin.path=/tmp/connect-plugins
plugin.discovery=service_load
connector.client.config.override.policy=All
EOF
exec /opt/kafka/bin/connect-distributed.sh /tmp/worker.properties
