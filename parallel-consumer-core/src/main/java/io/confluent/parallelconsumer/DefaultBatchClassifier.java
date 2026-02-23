package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Moniepoint, Inc.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class DefaultBatchClassifier<K, V> implements BatchClassifier<K, V> {
    @Override
    public boolean requiresUnique(final ConsumerRecord<K, V> r) {
        return true;
    }
}
