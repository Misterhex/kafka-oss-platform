#!/usr/bin/env bash
set -euo pipefail
mkdir -p /tmp/connect-plugins
cat >/tmp/worker.properties <<EOF
bootstrap.servers=${BOOTSTRAP}
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="platform" password="platform-lab";
producer.security.protocol=SASL_PLAINTEXT
producer.sasl.mechanism=PLAIN
producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="mirror" password="mirror-lab";
consumer.security.protocol=SASL_PLAINTEXT
consumer.sasl.mechanism=PLAIN
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="mirror" password="mirror-lab";
admin.security.protocol=SASL_PLAINTEXT
admin.sasl.mechanism=PLAIN
admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="platform" password="platform-lab";
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
