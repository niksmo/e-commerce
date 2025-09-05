# E-commerce

## Apply cluster A replication to B
```
curl -i -X PUT http://127.0.0.1:8082/connectors/mm2-src/config -H "Content-Type: application/json" -d '{
  "name": "mm2-src",
  "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
  "tasks.max": "2",
  "source.cluster.alias":"source",
  "source.cluster.bootstrap.servers": "kafka-a-1:9092,kafka-a-2:9092,kafka-a-3:9092",
  "target.cluster.bootstrap.servers": "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092",
  "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
  "topics": ".*",
  "emit.checkpoints.enabled": "true",
  "emit.heartbeats.enabled": "true",
  "sync.topic.configs.enabled": "true",
  "sync.topic.acls.enabled": "false",
  "refresh.topics.interval.seconds": "30",
  "refresh.groups.interval.seconds": "30"
}'
```

```
curl -i -X PUT http://127.0.0.1:8082/connectors/mm2-heartbeat/config -H "Content-Type: application/json" -d '{
  "name": "mm2-heartbeat",
  "connector.class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
  "tasks.max": "1",
  "source.cluster.alias":"source",
  "source.cluster.bootstrap.servers": "kafka-a-1:9092,kafka-a-2:9092,kafka-a-3:9092",
  "target.cluster.bootstrap.servers": "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092"
}'
```

```
curl -i -X PUT http://127.0.0.1:8082/connectors/mm2-checkpoint/config -H "Content-Type: application/json" -d '{
  "name": "mm2-checkpoint",
  "connector.class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
  "tasks.max": "1",
  "source.cluster.alias":"source",
  "source.cluster.bootstrap.servers": "kafka-a-1:9092,kafka-a-2:9092,kafka-a-3:9092",
  "target.cluster.bootstrap.servers": "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092",
  "emit.checkpoints.interval.seconds": "10"
}'
```


## Migrate database
```
./migrator --storage-path 'user:pass@127.0.0.1:5432/ecom?sslmode=disable' --migrations-path ./migrations
```

## Create topics
```
./topicmaker --config ./config.yaml
```

## Run application
```
./ecom --config ./config.yaml
```

