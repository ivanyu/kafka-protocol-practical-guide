.PHONY: kafka_docker
kafka_docker:
	docker run -ti --rm -p 9092:9092 apache/kafka:3.8.0

.PHONY: kafka_topic
kafka_topic:
	docker run --rm -ti --net=host apache/kafka:3.8.0 \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --create \
		--topic test-topic1 --partitions 2

.PHONY: test
test:
	python -m pytest .
