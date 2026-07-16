package com.example.producer;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * One production-shaped Kafka producer for the administration class.
 *
 * This is intentionally ordinary application code. The adjacent README.md tells
 * the trainer how to teach producer behavior by pointing at, or temporarily
 * changing, one setting at a time.
 *
 * Run:
 *   cd kafka-clients
 *   mvn -q compile exec:java -Dexec.mainClass=com.example.producer.KafkaProducerClient
 */
public class KafkaProducerClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerClient.class);

    // The consumer lesson reads from this topic. Create it before starting this client.
    private static final String TOPIC = "transactions";

    // Small vocabulary of realistic keys so partitioning demos have visible variety.
    private static final List<String> PAYMENT_TYPES = List.of(
            "neft", "rtgs", "imps", "upi",
            "credit_card", "debit_card", "wallet", "net_banking");

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();

        // ==================== 1. Connection and identity ====================

        // Seed brokers used to discover the cluster. The client learns all brokers later.
        // Give 2-3 addresses so one unavailable bootstrap broker does not block startup.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:19092,localhost:19093,localhost:19094");

        // One process identity, visible in client metrics, broker logs, and quota attribution.
        // Change this before attaching a per-client quota in scenario 14.
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "payments-producer-1");

        // ==================== 2. Serialization ====================

        // The application writes String keys and JSON text values, so both go out as String.
        // Kafka brokers store bytes; they do not validate application types or schemas.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // ==================== 3. Partitioning ====================

        // Default is the built-in sticky partitioner: null key = one partition per batch,
        // non-null key = hash(key) % partitions. Set partitioner.class only to override.
        // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
        //         "com.example.producer.PaymentTypePartitioner");

        // ==================== 4. Batching and throughput ====================

        // Bytes accumulated per partition batch before dispatch. Bigger = better throughput,
        // more memory pressure, and higher per-record latency. Scenario 7 alternates 16 KB / 128 KB.
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

        // Extra wait to let more records join a batch before dispatch (ms). A few ms often
        // doubles throughput under load. Scenario 5 (buffer full) uses 60_000 to force blocking.
        // props.put(ProducerConfig.LINGER_MS_CONFIG, "0");

        // Total memory the producer may use to buffer unsent records. When full, send() blocks
        // up to max.block.ms. Scenario 5 shrinks this to 32 KB to make blocking visible.
        // props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        // Compression codec applied per batch: none | gzip | snappy | lz4 | zstd. snappy and
        // lz4 are the fast defaults; zstd gives the best ratio. Scenario 8 rotates the four.
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        // Hard cap on a single produce request. It must stay below broker message.max.bytes;
        // raise both together if you need it. Scenario 6 shrinks this to trigger RecordTooLargeException.
        // props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");

        // ==================== 5. Reliability and ordering ====================

        // Replica acks required before a send is considered successful: 0 | 1 | all.
        // "all" pairs with idempotence to give exactly-once semantics. Scenario 9 rotates 0 / 1 / all.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Producer tags each batch with PID + sequence so brokers drop duplicate retries.
        // Setting this forces acks=all, retries>0, in-flight<=5. Scenario 11 flips it to "false".
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Automatic retry attempts on transient errors. Leave at MAX_VALUE and let
        // delivery.timeout.ms be the real ceiling.
        // props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

        // Wait between consecutive retry attempts for the same batch. Too low floods the broker
        // during an outage; 100 ms default is fine, 1 s is safer for chatty demos.
        // props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");

        // Max unacknowledged batches per connection in flight. With idempotence, up to 5 keeps
        // ordering; without, only 1 does.
        // props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // ==================== 6. Timeouts ====================

        // How long send() blocks when the buffer is full or metadata is missing.
        // Also bounds commitTransaction() wait. Scenario 5 uses 3_000 to make blocking visible.
        // props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");

        // Max time to wait for a broker response to a single produce request. Should exceed
        // broker replica.lag.time.max.ms to avoid spurious retries. Scenario 13 uses 1_000.
        // props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");

        // Upper bound on total time from send() to success or failure callback. It is the SLA
        // callers see; must be >= linger.ms + request.timeout.ms. Scenario 13 uses 3_000.
        // props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

        // ==================== 7. Interceptors ====================

        // Optional hooks that see records on send and again on acknowledgement.
        // Scenario 3 uncomments this to demo before/after logging on the producer I/O thread.
        // props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        //         "com.example.producer.LoggingProducerInterceptor");

        // ==================== 8. Transactions ====================

        // Stable producer identity the coordinator uses to fence older sessions. Setting this
        // enables EOS and forces acks=all, idempotence, in-flight<=5. Scenario 12 uses this
        // together with initTransactions() / beginTransaction() / commitTransaction().
        // props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "payments-producer-1-txid");

        // ==================== 9. Create, send, close ====================

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                TransactionEvent event = randomTransaction();
                String key = event.paymentType();
                String value = mapper.writeValueAsString(event);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                // Async send with a callback. The callback fires on the producer I/O thread
                // after the broker acks, or after the delivery attempt gives up.
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Send failed for key={}", key, exception);
                    } else {
                        log.info("Sent key={} partition={} offset={}",
                                key, metadata.partition(), metadata.offset());
                    }
                });

                //TimeUnit.MILLISECONDS.sleep(1000);
            }
        }
        // try-with-resources calls producer.close(), which flushes pending records and
        // waits for in-flight requests to complete before releasing resources.
        System.out.println("Producer closed cleanly.");
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

    /** Payload shape sent to the transactions topic. */
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
