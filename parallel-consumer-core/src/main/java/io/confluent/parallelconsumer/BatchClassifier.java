package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Moniepoint, Inc.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface BatchClassifier<K, V> {
    boolean requiresUnique(ConsumerRecord<K, V> r);
}
