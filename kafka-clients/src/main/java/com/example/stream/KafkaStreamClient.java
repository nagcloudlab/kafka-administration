package com.example.stream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One production-shaped Kafka Streams app for the administration class.
 *
 * Topology:
 *   transactions  ──[ filter key == "upi" ]──▶  upi-transactions
 *
 * Trainees need both topics to exist before starting (create upi-transactions
 * with the same partition count as transactions to preserve key-based ordering):
 *   ./kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
 *       --create --topic upi-transactions --partitions 3 --replication-factor 3
 *
 * Run (fast iteration — Ctrl+C may not run the shutdown hook cleanly because
 * Maven's exec:java shares its JVM with the plugin and terminates abruptly):
 *   cd kafka-clients
 *   mvn -q compile exec:java -Dexec.mainClass=com.example.stream.KafkaStreamsClient
 *
 * Run (clean SIGINT — streams.close() flushes state stores and sends LeaveGroup
 * to the coordinator, so rebalance is immediate):
 *   cd kafka-clients
 *   mvn -q compile
 *   mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt -Dmdep.pathSeparator=:
 *   java -cp "target/classes:$(cat cp.txt)" com.example.stream.KafkaStreamsClient
 */
public class KafkaStreamClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamClient.class);

    private static final String SOURCE_TOPIC = "transactions";
    private static final String SINK_TOPIC   = "upi-transactions";

    // Route we filter on. Every producer message with this key is forwarded.
    private static final String KEY_FILTER = "upi";

    public static void main(String[] args) {
        Properties props = new Properties();

        // ==================== 1. Connection and identity ====================

        // Seed brokers used to discover the cluster. The client learns all brokers later.
        // Give 2-3 addresses so one unavailable bootstrap broker does not block startup.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");

        // application.id doubles as the consumer group.id and the prefix for internal topics
        // and state store directories. It must be unique per logical Streams app in the cluster.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upi-transactions-router");

        // Base client identity, visible in metrics and broker logs. Streams appends
        // "-StreamThread-N-consumer" / "-producer" / "-restore-consumer" for each internal client.
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "upi-transactions-router-1");

        // ==================== 2. Serdes ====================

        // Default (de)serialization for keys and values. The topology may override per operator.
        // String on both sides matches what the producer writes and what the consumer expects.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // ==================== 3. Processing behavior ====================

        // Delivery semantics: at_least_once (default) | exactly_once_v2 (transactions on the
        // internal producer, needs a 3-broker cluster). Scenario 12 flips this to exactly_once_v2.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

        // Where to start reading when the app has no committed offset. earliest replays the
        // whole log on first launch, which is what a fresh training group wants to see.
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        // Frequency at which Streams commits offsets and flushes state stores. Lower =
        // shorter duplicate window on restart; higher = fewer commits + larger flushes.
        // props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "30000");

        // How many partitions of the input topics one JVM will process in parallel.
        // Increase to spread work; multiple JVMs with the same application.id divide partitions.
        // props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");

        // ==================== 4. State store location ====================

        // Local RocksDB directory used by KTable / windowed aggregations. Keep it on fast disk
        // in production; the training default is fine here.
        // props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        // ==================== 5. Topology ====================

        // Streams DSL: source -> filter -> sink. Same shape as consumer -> business logic ->
        // producer, but Streams handles group membership, commits, and restart-safe state.
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> transactions = builder.stream(SOURCE_TOPIC);

        transactions
                .peek((k, v) -> log.debug("in  key={} value={}", k, v))
                .filter((k, v) -> KEY_FILTER.equals(k))
                .peek((k, v) -> log.info("forwarding key={} value={}", k, v))
                .to(SINK_TOPIC);

        // ==================== 6. Lifecycle ====================

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Surface uncaught exceptions from stream threads instead of dying silently.
        // Default REPLACE_THREAD lets a crashed thread restart; SHUTDOWN_CLIENT stops the app.
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Uncaught exception in stream thread", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        // Log every state transition (CREATED → REBALANCING → RUNNING → ...).
        // Useful in class to point at what the coordinator and stream threads are doing.
        streams.setStateListener((newState, oldState) ->
                log.info("state {} -> {}", oldState, newState));

        // The shutdown hook must block until streams.close() finishes; otherwise the JVM
        // kills the stream threads before they flush state and send LeaveGroup, and the
        // coordinator has to wait session.timeout.ms before rebalancing.
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown hook: closing streams...");
            streams.close();
            shutdownLatch.countDown();
        }, "streams-shutdown-hook"));

        try {
            streams.start();
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Streams closed cleanly.");
    }
}
