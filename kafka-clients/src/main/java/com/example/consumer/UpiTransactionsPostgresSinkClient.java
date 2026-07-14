package com.example.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Sink consumer — reads upi-transactions, writes each record into PostgreSQL.
 *
 * Delivery semantics: at-least-once with idempotent upsert.
 *   1. poll() batch of records
 *   2. parse JSON → PreparedStatement.addBatch()
 *   3. Connection.commit()                    (durable in Postgres)
 *   4. Consumer.commitSync(offsets)           (durable in Kafka)
 * The Postgres upsert uses ON CONFLICT (txn_id) DO NOTHING so a replay of the
 * same records after a crash between step 3 and step 4 is a no-op.
 *
 * Pre-flight (run once before starting this client):
 *
 *   # 1. Postgres — either your own instance, or throwaway docker:
 *   docker run -d --name kafka-lab-pg \
 *       -e POSTGRES_USER=kafka_lab -e POSTGRES_PASSWORD=kafka_lab \
 *       -e POSTGRES_DB=kafka_lab -p 5432:5432 postgres:16
 *
 *   # 2. Table is auto-created on startup. Verify afterwards with:
 *   docker exec -it kafka-lab-pg psql -U kafka_lab -c 'select count(*) from upi_transactions;'
 *
 * Config via environment variables (defaults shown):
 *   POSTGRES_URL       jdbc:postgresql://localhost:5432/kafka_lab
 *   POSTGRES_USER      kafka_lab
 *   POSTGRES_PASSWORD  kafka_lab
 *
 * Run (clean SIGINT — recommended for this sink so the DB tx and Kafka commit
 * stay coordinated):
 *   cd kafka-clients
 *   mvn -q compile
 *   mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt -Dmdep.pathSeparator=:
 *   java -cp "target/classes:$(cat cp.txt)" com.example.consumer.UpiTransactionsPostgresSinkClient
 */
public class UpiTransactionsPostgresSinkClient {

    private static final Logger log = LoggerFactory.getLogger(UpiTransactionsPostgresSinkClient.class);

    private static final String TOPIC    = "upi-transactions";
    private static final String GROUP_ID = "upi-transactions-pg-sink";

    private static final String UPSERT_SQL = """
            INSERT INTO upi_transactions
                (txn_id, payment_type, amount, currency, from_account, to_account, status, event_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (txn_id) DO NOTHING
            """;

    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS upi_transactions (
                txn_id        VARCHAR(64)   PRIMARY KEY,
                payment_type  VARCHAR(32)   NOT NULL,
                amount        NUMERIC(18,2) NOT NULL,
                currency      VARCHAR(8)    NOT NULL,
                from_account  VARCHAR(32)   NOT NULL,
                to_account    VARCHAR(32)   NOT NULL,
                status        VARCHAR(32)   NOT NULL,
                event_ts      TIMESTAMP     NOT NULL,
                inserted_at   TIMESTAMP     NOT NULL DEFAULT NOW()
            )
            """;

    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception {

        // ==================== 1. Kafka connection and identity ====================

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,  GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "upi-transactions-pg-sink-1");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

        // ==================== 2. Deserialization ====================

        // Streams writes String key + JSON String value; we parse JSON with Jackson below.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // ==================== 3. Offsets and delivery behavior ====================

        // Manual commit — we commit only AFTER the Postgres transaction is durable.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");

        // ==================== 4. Group membership ====================

        // Short session so a Ctrl+C without LeaveGroup still rebalances within ~10 s.
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,   "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

        // ==================== 5. Postgres connection ====================

        String jdbcUrl  = env("POSTGRES_URL",      "jdbc:postgresql://localhost:5432/kafka_lab");
        String jdbcUser = env("POSTGRES_USER",     "kafka_lab");
        String jdbcPass = env("POSTGRES_PASSWORD", "kafka_lab");

        log.info("connecting to {} as {}", jdbcUrl, jdbcUser);
        try (Connection conn       = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Manual transaction control — each poll batch is one DB transaction.
            conn.setAutoCommit(false);
            createTableIfNeeded(conn);

            consumer.subscribe(List.of(TOPIC));
            log.info("subscribed to {} as group {}", TOPIC, GROUP_ID);

            // ==================== 6. Shutdown hook ====================

            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                RUNNING.set(false);
                consumer.wakeup();
                try { mainThread.join(15_000); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }, "pg-sink-shutdown-hook"));

            // ==================== 7. Poll → parse → batch UPSERT → DB commit → Kafka commit ====================

            ObjectMapper mapper = new ObjectMapper();
            Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();

            try (PreparedStatement upsert = conn.prepareStatement(UPSERT_SQL)) {
                while (RUNNING.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    if (records.isEmpty()) continue;

                    for (ConsumerRecord<String, String> record : records) {
                        bind(upsert, mapper.readTree(record.value()));
                        upsert.addBatch();

                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        processedOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    }

                    int[] counts = upsert.executeBatch();
                    conn.commit();                       // 1. durable in Postgres
                    consumer.commitSync(processedOffsets); // 2. durable in Kafka
                    log.info("upserted {} records ({} inserted, {} conflict/no-op)",
                            counts.length, countInserted(counts), countConflict(counts));
                    processedOffsets.clear();
                }
            } catch (WakeupException e) {
                if (RUNNING.get()) throw e;
            } catch (SQLException e) {
                log.error("SQL failure — rolling back Postgres tx; Kafka offsets stay uncommitted", e);
                conn.rollback();
                throw e;
            } finally {
                // Consumer close() sends LeaveGroup so the group rebalances immediately.
                consumer.close(Duration.ofSeconds(10));
                log.info("Sink closed cleanly.");
            }
        }
    }

    /** Bind one TransactionEvent JSON object to the UPSERT PreparedStatement. */
    private static void bind(PreparedStatement ps, JsonNode json) throws SQLException {
        ps.setString(1, json.get("txnId").asText());
        ps.setString(2, json.get("paymentType").asText());
        ps.setBigDecimal(3, json.get("amount").decimalValue());
        ps.setString(4, json.get("currency").asText());
        ps.setString(5, json.get("fromAccount").asText());
        ps.setString(6, json.get("toAccount").asText());
        ps.setString(7, json.get("status").asText());
        ps.setTimestamp(8, new Timestamp(json.get("timestamp").asLong()));
    }

    private static void createTableIfNeeded(Connection conn) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
            conn.commit();
        }
    }

    /** UPSERT rows that hit ON CONFLICT return 0 from executeBatch (per Postgres JDBC). */
    private static int countInserted(int[] counts) {
        int n = 0; for (int c : counts) if (c == 1) n++; return n;
    }
    private static int countConflict(int[] counts) {
        int n = 0; for (int c : counts) if (c == 0) n++; return n;
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }
}
