.PHONY: infra-start infra-stop infra-restart \
        compose-up wait-kafka kafka-permissions \
        spark-connect compose-down compose-clean \
		app-start

export ECOM_CONFIG_FILE=./config.yml

infra-start: compose-up wait-kafka kafka-permissions spark-connect

infra-stop: compose-down compose-clean

infra-restart: infra-stop infra-start

compose-up:
	docker compose up -d

wait-kafka:
	@echo "Waiting for Kafka to be ready..."
	@until docker compose exec kafka-a-1 \
	kafka-cluster cluster-id -c /etc/kafka/admin-client.properties \
	-b localhost:9092 >/dev/null 2>&1; \
	do sleep 2; done
	@echo "Kafka is ready!"

kafka-permissions:
	docker compose exec kafka-a-1 kafka-acls --bootstrap-server localhost:9092 \
	--command-config /etc/kafka/admin-client.properties \
  	--add --allow-principal User:app --operation all \
  	--topic products-from-shop \
  	--topic products-to-storage \
  	--topic filter-product-stream \
  	--topic filter-product-group-table \
  	--topic filter-product-group \
  	--topic client-find-product-events
	--topic product-offers

spark-connect:
	docker compose exec spark-master sbin/start-connect-server.sh \
	--packages org.apache.spark:spark-connect_2.12:3.5.2

compose-down:
	docker compose down

compose-clean:
	yes | docker volume prune

app-start:
	go run ./cmd/topicmaker/. && \
	go run ./cmd/migrator/. \
	--storage-path 'user:pass@127.0.0.1:5432/ecom?sslmode=disable' \
	--migrations-path ./migrations && \
	go run ./cmd/ecom/.
