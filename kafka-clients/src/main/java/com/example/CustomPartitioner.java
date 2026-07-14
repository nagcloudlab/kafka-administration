package com.example;

public class CustomPartitioner implements org.apache.kafka.clients.producer.Partitioner {

    public void configure(java.util.Map<String, ?> configs) {
        // no-op
    }
   
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, org.apache.kafka.common.Cluster cluster) {
        // custom partitioning logic based on the key (payment type)
        String paymentType = (String) key;
        switch (paymentType) {
            case "neft":
                return 0; // send to partition 0
            case "rtgs":
                return 1; // send to partition 1
            case "imps":
                return 2; // send to partition 2
            case "upi":
                return 0; // send to partition 0
            case "credit_card":
                return 2; // send to partition 2
            case "debit_card":
                return 2; // send to partition 2
            case "wallet":
                return 2; // send to partition 2
            case "net_banking":
                return 1; // send to partition 1
            default:
                return 0; // default to partition 0 if payment type is unknown
        }
    }

    public void close() {
        // cleanup resources if needed
    }
}