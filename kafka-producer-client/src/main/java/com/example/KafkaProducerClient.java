package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.TimeUnit;

public class KafkaProducerClient {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        // ---------- 1. Connection ----------
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094"); // seed brokers used to discover
                                                                                        // the full cluster
        props.put("client.id", "payments-producer-1"); // logical name of this producer — appears in broker logs, JMX
                                                       // metrics, and quota tracking

        // ---------- 2. Serialization ----------
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // how the record key is
                                                                                               // turned into bytes
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // how the record value
                                                                                                 // is turned into bytes

        // ---------- 3. Partitioning ----------
        props.put("partitioner.class", "com.example.CustomPartitioner"); // custom class that decides which partition
                                                                         // each record goes to

        // ---------- 4. Batching & throughput ----------
        props.put("batch.size", 16384); // max bytes accumulated per partition batch before it's sent (16 KB)
        props.put("linger.ms", 5); // extra wait to let more records join a batch — trades a bit of latency for
                                   // throughput
        props.put("buffer.memory", 33554432); // total memory the producer can use to buffer unsent records (32 MB)
        props.put("max.block.ms", 60000); // how long send() will block when the buffer is full before throwing
        props.put("compression.type", "snappy"); // compress each batch — options: none | gzip | snappy | lz4 | zstd
        props.put("max.request.size", 1048576); // hard cap on the size of a single produce request (1 MB) — must be <
                                                // broker's message.max.bytes

        // ---------- 5. Reliability — acks, retries, idempotence ----------
        props.put("acks", "all"); // wait for leader + all in-sync replicas to ack (strongest durability)
        props.put("retries", Integer.MAX_VALUE); // retry transient send failures effectively forever (bounded by
                                                 // delivery.timeout.ms)
        props.put("retry.backoff.ms", 1000); // wait 1s between retry attempts for the same batch
        props.put("enable.idempotence", "true"); // dedupe retries so at-least-once retries don't produce duplicates
        props.put("max.in.flight.requests.per.connection", 5); // up to 5 unacked batches per connection — safe to keep
                                                               // ordering under idempotence

        // ---------- 6. Timeouts ----------
        props.put("request.timeout.ms", 30000); // max time to wait for a broker response to a single produce request
        props.put("delivery.timeout.ms", 120000); // upper bound on total time from send() to success/failure (must be
                                                  // >= linger.ms + request.timeout.ms)

        // ---------- 7. Interceptors ----------
        // props.put("interceptor.classes", "com.example.LoggingProducerInterceptor");
        // // cross-cutting hooks — run on every send() and every ack/error (audit,
        // tracing, metrics)

        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // create a Kafka producer

        for (int i = 0; i < 100; i++) {
            // send messages to a Kafka topic
            String topic = "transactions";
            int partition = 0; // specify the partition to send the message to

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
            TimeUnit.MILLISECONDS.sleep(1000); // sleep for 1 seconds before sending the next message
        }
        producer.close();
    }
}