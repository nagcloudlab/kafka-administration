package com.example;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerClient {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        // ==================== 1. Connection ====================

        // - Comma-separated seed brokers used to discover full cluster metadata.
        // - 2-3 entries is enough; client learns the rest at runtime.
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

        // - Logical name of this producer, surfaced in broker logs and JMX metrics.
        // - Used by brokers to attribute traffic and apply per-client quotas.
        props.put("client.id", "payments-producer-1");

        // ==================== 2. Serialization ====================

        // - Class that converts the record key object into bytes on the wire.
        // - Must match what consumers deserialize with (String/Avro/Protobuf/JSON).
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // - Class that converts the record value object into bytes on the wire.
        // - For typed payloads prefer schema-based formats (Avro/Protobuf) +
        // compression.
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // ==================== 3. Partitioning ====================

        // - Custom Partitioner class that picks a partition for each record.
        // - Omit to use the default sticky partitioner; set only for key-based routing.
        props.put("partitioner.class", "com.example.CustomPartitioner");

        // ==================== 4. Batching & Throughput ====================

        // - Max bytes accumulated per partition batch before it's sent (16 KB here).
        // - Bigger = better throughput but more memory and higher per-record latency.
        props.put("batch.size", 16384);

        // - Extra wait to let more records join a batch before dispatch (ms).
        // - 0 = send ASAP; a few ms often doubles throughput under load.
        props.put("linger.ms", 5);

        // - Total memory the producer can use to buffer unsent records (32 MB here).
        // - When full, send() blocks up to max.block.ms; size for peak burst.
        props.put("buffer.memory", 33554432);

        // - Compression codec per batch: none | gzip | snappy | lz4 | zstd.
        // - snappy/lz4 = fast; zstd = best ratio; broker must support the codec.
        props.put("compression.type", "snappy");

        // - Hard cap on the size of a single produce request in bytes (1 MB here).
        // - Must be < broker's message.max.bytes; raise both together if needed.
        props.put("max.request.size", 1048576);

        // ==================== 5. Reliability ====================

        // - Replica acks required before a send is considered successful: 0 | 1 | all.
        // - "all" = strongest durability; pair with idempotence for exactly-once.
        props.put("acks", "all");

        // - Producer tags batches with PID + sequence so brokers drop duplicates.
        // - Required for exactly-once; forces acks=all, retries>0, in-flight<=5.
        props.put("enable.idempotence", "true");

        // - Number of automatic retry attempts on transient errors.
        // - Set MAX_VALUE and let delivery.timeout.ms bound total time.
        props.put("retries", Integer.MAX_VALUE);

        // - Wait between consecutive retry attempts for the same batch (ms).
        // - Too low = broker flood on outage; 1s is a safe default.
        props.put("retry.backoff.ms", 1000);

        // - Max unacknowledged batches per connection in flight at once.
        // - With idempotence, up to 5 keeps ordering; without, only 1 does.
        props.put("max.in.flight.requests.per.connection", 5);

        // ==================== 6. Timeouts ====================

        // - How long send() blocks when buffer is full or metadata missing.
        // - Low = fail fast for latency-sensitive callers; high = background jobs.
        props.put("max.block.ms", 60000);

        // - Max time to wait for a broker response to a single produce request.
        // - Should exceed broker replica.lag.time.max.ms to avoid spurious retries.
        props.put("request.timeout.ms", 30000);

        // - Upper bound on total time from send() to success/failure callback.
        // - Must be >= linger.ms + request.timeout.ms; this is the SLA callers see.
        props.put("delivery.timeout.ms", 120000);

        // ==================== 7. Interceptors ====================

        // - Comma-separated ProducerInterceptor classes hooked into send + ack.
        // - Cross-cutting concerns (audit, tracing, metrics) without touching business
        // code.
        // props.put("interceptor.classes", "com.example.LoggingProducerInterceptor");

        // create a Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            // send messages to a Kafka topic
            String topic = "transactions";
            // specify the partition to send the message to
            int partition = 0;

            // generate 1kb size message, no key
            String value = "This is a sample message of size 1KB. ".repeat(64); // 64 * 16 bytes = 1024 bytes = 1KB
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, null, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.println("Message sent to topic: " + metadata.topic() + ", partition: "
                            + metadata.partition() + ", offset: " + metadata.offset());
                }
            });
            // sleep for 1 seconds before sending the next message
            TimeUnit.MILLISECONDS.sleep(1000);
        }
        producer.close();
    }
}
