# kafka-stream-WordCount
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-input
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-output

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic word-count-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserialzer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic word-count-input

java -jar com.kafka.stream-1.0-SNAPSHOT-jar-with-dependencies.jar
