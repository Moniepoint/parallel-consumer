package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface BatchClassifier<K, V> {
    boolean requiresUnique(ConsumerRecord<K, V> r);
}
