package com.example.producer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * ONE file, ONE walkthrough — every producer concept in a single class.
 *
 * The `README.md` guide next to this file walks through each concept and tells
 * the trainer exactly which lines below to edit.
 *
 * The run command is always the same:
 *   cd kafka-clients
 *   mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
 *
 * Scenarios (see the guide for exact edits):
 *   1.  Baseline — full-config walkthrough
 *   2.  Partitioning by payment type            (nested PaymentTypePartitioner)
 *   3.  Interceptor hooks                       (uncomment interceptor.classes below)
 *   4.  Broker down / metadata refresh          (kill a broker mid-run)
 *   5.  Buffer full → max.block.ms              (shrink buffer.memory + huge linger)
 *   6.  Record too big                          (shrink max.request.size)
 *   7.  Batching sweep — linger.ms × batch.size
 *   8.  Compression codec — none/snappy/lz4/zstd
 *   9.  acks=0 vs 1 vs all                      (change acks + kill leader)
 *   10. min.insync.replicas floor               (alter topic config + stop a broker)
 *   11. Idempotence toggle                      (flip enable.idempotence)
 *   12. Transactions                            (wrap the send loop in beginTransaction/commit)
 *   13. Timeout hierarchy                       (tighten 3 timeouts + alter topic config)
 *   14. Client quota / throttle                 (change client.id + attach quota via kafka-configs.sh)
 *   15. JMX metrics                             (attach JConsole; no code change)
 *   16. Benchmarking                            (kafka-producer-perf-test.sh CLI; no code change)
 *   17. Schema Registry / Avro                  (code walkthrough; requires Schema Registry service)
 *   18. Graceful shutdown                       (change loop count + close(Duration))
 */
public class KafkaProducerClient {

    // One topic across every scenario. Scenarios 10 and 13 tweak topic-level
    // config on this same topic via `kafka-configs.sh --alter`.
    private static final String TOPIC = "transactions";

    private static final List<String> PAYMENT_TYPES = List.of(
            "neft", "rtgs", "imps", "upi",
            "credit_card", "debit_card", "wallet", "net_banking");

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        // ==================== 1. Connection ====================

        // - Comma-separated seed brokers used to discover full cluster metadata.
        // - 2-3 entries is enough; client learns the rest at runtime.
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

        // - Logical name of this producer, surfaced in broker logs and JMX metrics.
        // - Used by brokers to attribute traffic and apply per-client quotas.
        //   For scenario 14 (quota), change to `quota-demo-client` before attaching
        //   `producer_byte_rate=8192` via kafka-configs.sh.
        props.put("client.id", "payments-producer-1");

        // ==================== 2. Serialization ====================

        // - Class that converts the record key object into bytes on the wire.
        // - Must match what consumers deserialize with.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // - Class that converts the record value object into bytes on the wire.
        // - For typed payloads prefer schema-based formats (Avro/Protobuf) + compression.
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // ==================== 3. Partitioning ====================

        // - Nested PaymentTypePartitioner below routes by business meaning.
        // - Omit to use the default sticky partitioner; keep only for key-based routing.
        props.put("partitioner.class",
                "com.example.producer.KafkaProducerClient$PaymentTypePartitioner");

        // ==================== 4. Batching & Throughput ====================

        // - Max bytes accumulated per partition batch before it's sent (16 KB here).
        // - Bigger = better throughput but more memory and higher per-record latency.
        //   Scenario 7 (batching sweep) alternates 16 KB and 128 KB.
        props.put("batch.size", 16384);

        // - Extra wait to let more records join a batch before dispatch (ms).
        // - 0 = send ASAP; a few ms often doubles throughput under load.
        //   Scenario 7 alternates 0 / 5 / 50; scenario 5 (buffer full) uses 60_000.
        props.put("linger.ms", 5);

        // - Total memory the producer can use to buffer unsent records (32 MB here).
        // - When full, send() blocks up to max.block.ms.
        //   Scenario 5 shrinks this to 32 KB to force blocking.
        props.put("buffer.memory", 33554432);

        // - Compression codec per batch: none | gzip | snappy | lz4 | zstd.
        // - snappy/lz4 = fast; zstd = best ratio; broker must support the codec.
        //   Scenario 8 rotates through the four codecs.
        props.put("compression.type", "snappy");

        // - Hard cap on the size of a single produce request in bytes (1 MB here).
        // - Must be < broker's message.max.bytes; raise both together if needed.
        //   Scenario 6 shrinks this to 1024 to trigger RecordTooLargeException.
        props.put("max.request.size", 1048576);

        // ==================== 5. Reliability ====================

        // - Replica acks required before a send is considered successful: 0 | 1 | all.
        // - "all" = strongest durability; pair with idempotence for exactly-once.
        //   Scenario 9 rotates through 0 / 1 / all.
        props.put("acks", "all");

        // - Producer tags batches with PID + sequence so brokers drop duplicate retries.
        // - Required for exactly-once; forces acks=all, retries>0, in-flight<=5.
        //   Scenario 11 flips this to "false" to see duplicates after a broker blip.
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
        // - Low = fail fast; high = background-job producers.
        //   Scenario 5 (buffer full) uses 3_000 to make blocking visible in seconds.
        props.put("max.block.ms", 60000);

        // - Max time to wait for a broker response to a single produce request.
        // - Should exceed broker `replica.lag.time.max.ms` to avoid spurious retries.
        //   Scenario 13 uses 1_000 to trigger visible per-request timeouts.
        props.put("request.timeout.ms", 30000);

        // - Upper bound on total time from send() to success/failure callback.
        // - Must be >= linger.ms + request.timeout.ms; this is the SLA callers see.
        //   Scenario 13 uses 3_000 to make the batch-expired path fire.
        props.put("delivery.timeout.ms", 120000);

        // ==================== 7. Interceptors ====================

        // - Nested LoggingProducerInterceptor below prints on send + ack.
        // - Uncomment for scenario 3 to see the two hook lines interleave with sends.
        // props.put("interceptor.classes",
        //         "com.example.producer.KafkaProducerClient$LoggingProducerInterceptor");

        // ==================== 8. Optional: transactions (scenario 12) ====================
        //
        // For scenario 12, ADD this line above:
        //     props.put("transactional.id", "demo-tx-1");
        //
        // Then wrap the send loop below like this (both sends go to TOPIC):
        //     producer.initTransactions();
        //     for (int i = 0; i < 10; i++) {
        //         producer.beginTransaction();
        //         String key = "ord-" + i;
        //         producer.send(new ProducerRecord<>(TOPIC, key, "order:" + i));
        //         producer.send(new ProducerRecord<>(TOPIC, key, "created:" + i));
        //         if (i % 2 == 0) producer.commitTransaction();
        //         else            producer.abortTransaction();
        //     }
        //
        // Then compare two consumers:
        //   ./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning
        //   ./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --isolation-level read_committed

        // ==================== Producer loop ====================

        ObjectMapper mapper = new ObjectMapper();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            TransactionEvent event = randomTransaction();
            String key = event.paymentType();
            String value = mapper.writeValueAsString(event);

            List<Header> headers = List.of(
                    header("event-type",     "payment.initiated"),
                    header("source",         "payments-producer"),
                    header("correlation-id", UUID.randomUUID().toString()),
                    header("content-type",   "application/json"),
                    header("schema-version", "1"));

            // partition = null lets PaymentTypePartitioner route by key (paymentType).
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC, null, System.currentTimeMillis(), key, value, headers);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.println("Sent key=" + key
                            + " topic=" + metadata.topic()
                            + " partition=" + metadata.partition()
                            + " offset=" + metadata.offset());
                }
            });

            TimeUnit.MILLISECONDS.sleep(1000);
        }

        producer.close();
    }

    private static TransactionEvent randomTransaction() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String paymentType = PAYMENT_TYPES.get(rnd.nextInt(PAYMENT_TYPES.size()));
        double amount = Math.round(rnd.nextDouble(100.0, 100000.0) * 100.0) / 100.0;
        return new TransactionEvent(
                UUID.randomUUID().toString(),
                paymentType, amount, "INR",
                "ACC" + rnd.nextInt(1000, 9999),
                "ACC" + rnd.nextInt(1000, 9999),
                "INITIATED",
                System.currentTimeMillis());
    }

    private static Header header(String name, String value) {
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }

    // ================================================================================
    // Nested SPI classes — kept in the same file so trainees see ONE thing on screen.
    // ================================================================================

    /**
     * Routes payment types to fixed partitions.
     * Wired via `partitioner.class = com.example.producer.KafkaProducerClient$PaymentTypePartitioner`.
     */
    public static class PaymentTypePartitioner implements Partitioner {

        public void configure(Map<String, ?> configs) { /* no-op */ }

        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, byte[] valueBytes, Cluster cluster) {
            String paymentType = (String) key;
            switch (paymentType) {
                case "neft":        return 0;
                case "rtgs":        return 1;
                case "imps":        return 2;
                case "upi":         return 0;
                case "credit_card": return 2;
                case "debit_card":  return 2;
                case "wallet":      return 2;
                case "net_banking": return 1;
                default:            return 0;
            }
        }

        public void close() { /* no-op */ }
    }

    /**
     * Logs on both `onSend` (caller thread) and `onAcknowledgement` (producer I/O thread).
     * Wired via `interceptor.classes = com.example.producer.KafkaProducerClient$LoggingProducerInterceptor`.
     * The line stays commented out in main(); scenario 3 in the guide uncomments it.
     */
    public static class LoggingProducerInterceptor implements ProducerInterceptor<String, String> {

        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            System.out.println("[interceptor] onSend  topic=" + record.topic()
                    + " key=" + record.key()
                    + " valueBytes=" + (record.value() == null ? 0 : record.value().length()));
            return record;
        }

        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                System.err.println("[interceptor] onAck  ERROR: " + exception.getMessage());
            } else {
                System.out.println("[interceptor] onAck  topic=" + metadata.topic()
                        + " partition=" + metadata.partition()
                        + " offset=" + metadata.offset());
            }
        }

        public void close() { /* no-op */ }

        public void configure(Map<String, ?> configs) { /* no-op */ }
    }

    /**
     * Payload shape sent to the transactions topic.
     * Nested record — no separate file needed.
     */
    public record TransactionEvent(
            String txnId,
            String paymentType,
            double amount,
            String currency,
            String fromAccount,
            String toAccount,
            String status,
            long timestamp) {}
}
