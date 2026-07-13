package com.example;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class LoggingProducerInterceptor implements ProducerInterceptor<String, String> {

    // called on the caller's thread just before send() hands the record to the accumulator
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        System.out.println("[interceptor] onSend  topic=" + record.topic()
                + " key=" + record.key()
                + " valueBytes=" + (record.value() == null ? 0 : record.value().length()));
        return record; // returning a different record here would rewrite what actually gets sent
    }

    // called on the producer I/O thread once the broker acks (or the send fails)
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            System.err.println("[interceptor] onAck  ERROR: " + exception.getMessage());
        } else {
            System.out.println("[interceptor] onAck  topic=" + metadata.topic()
                    + " partition=" + metadata.partition()
                    + " offset=" + metadata.offset());
        }
    }

    public void close() {
        // no resources to release
    }

    public void configure(Map<String, ?> configs) {
        // no-op — could read custom keys from producer props here
    }
}
