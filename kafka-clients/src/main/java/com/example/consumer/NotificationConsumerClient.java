package com.example.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * One production-shaped Kafka consumer for the administration class.
 *
 * This is intentionally ordinary application code. The adjacent README.md tells
 * the trainer how to teach consumer behavior by pointing at, or temporarily
 * changing, one setting at a time.
 *
 * Run (fast iteration — Ctrl+C may not run the shutdown hook cleanly because
 * Maven's exec:java shares its JVM with the plugin and terminates abruptly on
 * SIGINT; the coordinator then waits session.timeout.ms before rebalancing):
 *   cd kafka-clients
 *   mvn -q compile exec:java -Dexec.mainClass=com.example.consumer.KafkaConsumerClient
 *
 * Run (clean SIGINT — shutdown hook runs, close() sends LeaveGroup, coordinator
 * rebalances immediately; recommended for scenarios 2, 3, 4, 10, 18):
 *   cd kafka-clients
 *   mvn -q compile
 *   mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt -Dmdep.pathSeparator=:
 *   java -cp "target/classes:$(cat cp.txt)" com.example.consumer.KafkaConsumerClient
 */
public class NotificationConsumerClient {

    // The producer lesson writes to this topic. Create it before starting this client.
    private static final String TOPIC = "transactions";

    // Used by the shutdown hook and the main poll loop.
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NotificationConsumerClient.class);

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        // ==================== 1. Connection and identity ====================

        // Seed brokers used to discover the cluster. The client learns all brokers later.
        // Give 2-3 addresses so one unavailable bootstrap broker does not block startup.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");

        // The logical application identity. Members with the same group.id divide partitions.
        // Committed offsets belong to this group, not to an individual consumer process.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "txn-notification-group");

        // One process identity, visible in client metrics, broker logs, and quota attribution.
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "txn-notification-client");

        // A misspelled topic must fail instead of being silently created with broker defaults.
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

        // ==================== 2. Deserialization ====================

        // The producer writes String keys and JSON text values, so both are read as String.
        // Kafka brokers store bytes; they do not validate application types or schemas.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // ==================== 3. Offsets and delivery behavior ====================

        
        // Used only when this group has no valid committed offset.
        // Alternatives for the lesson: earliest | latest | none.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // from beginning of log if no committed offset


        // Manual commit: the application commits only after processing succeeds.
        // This creates at-least-once delivery: a crash before commit can repeat records.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Hide records from aborted transactions. Non-transactional records remain visible.
        // Change to read_uncommitted to see every physical data record in the log.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // ==================== 4. Group membership and rebalancing ====================

        // Missing heartbeats for this long causes the coordinator to remove the member.
        // The default is 45_000; a lower value shortens the rebalance delay when a
        // consumer dies without sending LeaveGroup (Ctrl+C under mvn exec:java, kill -9).
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");

        // Classic-protocol heartbeat frequency; keep comfortably below session timeout.
        // Rule of thumb: heartbeat.interval.ms <= session.timeout.ms / 3.
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

        // Maximum allowed time between poll() calls. This protects against stuck processing.
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");

        // Easy to visualize in class. Compare CooperativeStickyAssignor during the lesson.
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        // Optional static membership: uncomment with a UNIQUE stable ID per live instance.
        // Never copy the same ID to two running processes; the old member is fenced.
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "txn-notification-instance-"+System.getenv("INSTANCE_ID"));

        // ==================== 5. Fetching and processing capacity ====================

        // Maximum records returned by one poll. It bounds work count, not response bytes.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        // Broker may wait for this many bytes before returning a fetch response.
        // Raise for throughput batching; keep low for latency-sensitive consumers.
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");

        // Maximum broker wait for fetch.min.bytes before responding anyway.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");

        // Maximum bytes fetched from one partition. It must fit the largest legal record.
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");

        // Soft maximum bytes across all partitions in one fetch response.
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800");

        // ==================== 6. Create, subscribe, poll, process, commit ====================

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();

        // subscribe() uses group coordination and automatic partition assignment.
        consumer.subscribe(List.of(TOPIC), new LoggingRebalanceListener(processedOffsets));

        // wakeup() is the Kafka-supported way to interrupt a blocking poll from another thread.
        // The hook must join() the main thread — the JVM kills user threads as soon as all
        // shutdown hooks return, so without join() the finally-block close() gets cut off
        // and no LeaveGroup is sent (the coordinator would then wait session.timeout.ms).
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            RUNNING.set(false);
            consumer.wakeup();
            try {
                mainThread.join(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "consumer-shutdown-hook"));

        long pollNumber = 0;
        try {
            while (RUNNING.get()) {
                // poll() joins/maintains the group and returns a batch of records.
                //log.info("polling for records...");
                var records = consumer.poll(Duration.ofSeconds(1)); // Fetch Request
                //log.info("received {} records", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    process(record);

                    // Store the NEXT offset only after this record was processed successfully.
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    processedOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));

                    //TimeUnit.MILLISECONDS.sleep(3000);
                }

                // how to prevent duplicate processing on crash
                // => idempotent processing: design process() to be idempotent, so that re-processing the same record has no effect
                // -> upsert with external storage (db, file, etc.) to store the last processed offset for each partition
                // -> we can use in-memory data structure to store the last processed offset for each partition, and persist it to external storage periodically

                // use time based commit to commit offsets periodically, e.g. every 10 seconds
                // use count based commit to commit offsets after processing a certain number of records, e.g, every 100 records
                // use a combination of both time and count based commit to commit offsets, e.g. every 10 seconds or every 100 records, whichever comes first
                // use record by record commit to commit offsets after processing each record, but this can be inefficient and slow down processing

                if (!processedOffsets.isEmpty()) {
                    // Synchronous commit is simple and reports failure to this thread.
                    // A crash after process() but before this commit causes duplicate delivery.
                    consumer.commitSync(processedOffsets); // Commit Request
                    processedOffsets.clear();
                }

                // Print client-side position lag occasionally for classroom visibility.
                if (++pollNumber % 10 == 0 && !consumer.assignment().isEmpty()) {
                    printPositionLag(consumer);
                }
            }
        } catch (WakeupException e) {
            if (RUNNING.get()) throw e; // unexpected wakeup; do not hide a real failure
        } finally {
            // close() leaves the group cleanly and releases network resources.
            consumer.close(Duration.ofSeconds(10));
            System.out.println("Consumer closed cleanly.");
        }
    }

    /** Represents successful business processing. */
    private static void process(ConsumerRecord<String, String> record) {
        log.info("received topic={} partition={} offset={} key={} value={}",
                record.topic(), record.partition(), record.offset(),
                record.key(), record.value());
    }

    /** Live position lag; admin group lag normally uses the group's committed offset. */
    private static void printPositionLag(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
        for (TopicPartition tp : sorted(consumer.assignment())) {
            long position = consumer.position(tp);
            long logEnd = endOffsets.get(tp);
            log.info("lag {} position={} logEnd={} recordsBehind={}",
                    tp, position, logEnd, logEnd - position);
        }
    }

    /** Makes partition ownership changes visible during joins, leaves, and failures. */
    private static class LoggingRebalanceListener implements ConsumerRebalanceListener {
        private final Map<TopicPartition, OffsetAndMetadata> processedOffsets;

        private LoggingRebalanceListener(Map<TopicPartition, OffsetAndMetadata> processedOffsets) {
            this.processedOffsets = processedOffsets;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // Ownership is being handed off normally. The main loop commits every batch,
            // so this map is normally empty; real applications often commit/checkpoint here.
            System.out.println("REVOKED  " + format(partitions)
                    + " pendingOffsets=" + processedOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("ASSIGNED " + format(partitions));
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            // Ownership is already gone. Do not commit partition work as if still owner.
            System.out.println("LOST     " + format(partitions));
            processedOffsets.keySet().removeAll(partitions);
        }
    }

    private static List<TopicPartition> sorted(Collection<TopicPartition> partitions) {
        return partitions.stream()
                .sorted(Comparator.comparing(TopicPartition::toString))
                .toList();
    }

    private static String format(Collection<TopicPartition> partitions) {
        return sorted(partitions).stream()
                .map(TopicPartition::toString)
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
