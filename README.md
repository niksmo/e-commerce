# E-commerce


## Set permission for 
```
docker compose exec kafka-a-1 kafka-acls --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin-client.properties \
  --add --allow-principal User:app --producer \
  --topic products-from-shop \
  --topic products-to-storage \
  --topic filter-product-stream \
  --topic filter-product-group-table \
  --topic client-find-product-events

docker compose exec kafka-a-1 kafka-acls --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin-client.properties \
  --add --allow-principal User:app --consumer \
  --topic products-from-shop --group product-blocker-group \
  --topic products-to-storage --group product-saver-group \
  --topic filter-product-stream --group filter-product-group \
  --topic filter-product-group-table --group product-blocker-group \
  --topic client-find-product-events --group client-events-group
```

## Apply cluster A replication to B
```
curl -i -X POST http://127.0.0.1:8082/connectors -H "Content-Type: application/json" -d '{
    "name": "mirror_A_to_B",
    "config": {
        "name": "mirror_A_to_B",
        "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
        "tasks.max": "2",
        "source.cluster.alias": "source",
        "source.cluster.bootstrap.servers": "kafka-a-1:9092,kafka-a-2:9092,kafka-a-3:9092",
        "source.cluster.security.protocol": "SASL_SSL",
        "source.cluster.sasl.mechanism": "PLAIN",
        "source.cluster.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";",
        "source.cluster.ssl.truststore.location": "/etc/kafka/secrets/kafka.truststore.jks",
        "source.cluster.ssl.truststore.password": "123456",
        "source.cluster.ssl.keystore.location": "/etc/kafka/secrets/connect-mm2.keystore.jks",
        "source.cluster.ssl.keystore.password": "123456",
        "target.cluster.bootstrap.servers": "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092",
        "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
        "topics": ".*",
        "emit.checkpoints.enabled": "true",
        "emit.heartbeats.enabled": "true",
        "sync.topic.configs.enabled": "true",
        "sync.topic.acls.enabled": "false",
        "refresh.topics.interval.seconds": "30",
        "refresh.groups.interval.seconds": "30"
    }
}'
```

```
curl -i -X POST http://127.0.0.1:8082/connectors -H "Content-Type: application/json" -d '{
  "name": "mm2-heartbeat",
  "config": {
    "connector.class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
    "tasks.max": "1",
    "source.cluster.alias":"source",
    "source.cluster.bootstrap.servers": "kafka-a-1:9092,kafka-a-2:9092,kafka-a-3:9092",
    "target.cluster.bootstrap.servers": "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092"
  }
}'
```

```
curl -i -X POST http://127.0.0.1:8082/connectors -H "Content-Type: application/json" -d '{
  "name": "mm2-checkpoint",
  "config": {
    "connector.class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
    "tasks.max": "1",
    "source.cluster.alias":"source",
    "source.cluster.bootstrap.servers": "kafka-a-1:9092,kafka-a-2:9092,kafka-a-3:9092",
    "target.cluster.bootstrap.servers": "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092",
    "emit.checkpoints.interval.seconds": "10"
  }
}'
```

## Build applications:

```
# TODO
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

