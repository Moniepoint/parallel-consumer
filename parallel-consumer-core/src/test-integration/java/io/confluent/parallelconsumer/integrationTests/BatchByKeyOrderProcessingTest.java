
/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 */

package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.RecordContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.BATCH_BY_KEY;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Tests around what should happen when rebalancing occurs
 */
@Slf4j
class BatchByKeyOrderProcessingTest extends BrokerIntegrationTest<String, String> {
    private static final int BATCH_SIZE = 10;
    private static final int SINGLE_KEY_BATCH_SIZE = 20;

    Consumer<String, String> consumer;

    ParallelEoSStreamProcessor<String, String> pc;

    {
        super.numPartitions = 5;
    }

    @BeforeEach
    void setup() {
        setupTopic();
        consumer = getKcu().createNewConsumer(true, consumerProps());
    }

    @AfterEach
    void cleanup() {
        pc.close();
    }

    private ParallelEoSStreamProcessor<String, String> setupPC() {
        return setupPC(null);
    }

    private ParallelEoSStreamProcessor<String, String> setupPC(Function<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>, ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> optionsCustomizer) {
        ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder =
                ParallelConsumerOptions.<String, String>builder()
                        .consumer(consumer)
                        .ordering(BATCH_BY_KEY)
                        .batchSize(BATCH_SIZE)
                        .singleKeyBatchSize(SINGLE_KEY_BATCH_SIZE)
                        .maxConcurrency(5);
        if (optionsCustomizer != null) {
            optionsBuilder = optionsCustomizer.apply(optionsBuilder);
        }

        return new ParallelEoSStreamProcessor<>(optionsBuilder.build());
    }

    @SneakyThrows
    @Test
    void allPartitionsAreProcessedInParallel() {
        var numberOfRecordsToProduce = 10000L;
        Map<Integer, AtomicInteger> partitionCounts = new HashMap<>();
        IntStream.range(0, 5).forEach(part -> partitionCounts.put(part, new AtomicInteger(0)));
        pc = setupPC(options -> options.messageBufferSize(5000)); // Increasing message buffer to 10 * max partition fetch - to make sure mix of data from all partitions is available for processing
        pc.subscribe(UniSets.of(topic));

        //
        getKcu().produceMessages(topic, numberOfRecordsToProduce, "", 50);

        // consume all the messages
        var inProcess = new HashMap<String, AtomicBoolean>();
        pc.poll(recordContexts -> {
            var records = recordContexts.stream().toList();
            var key = records.getFirst().key();
            Assertions.assertTrue(SINGLE_KEY_BATCH_SIZE >= records.size(), "Batch size exceeds configured batch limit");
            Assertions.assertFalse(inProcess.getOrDefault(key, new AtomicBoolean(false)).get(), "key is in progress in multiple threads");
            inProcess.put(key, new AtomicBoolean(true));

            recordContexts.stream().forEach(context -> {
                        partitionCounts.get(context.partition()).getAndIncrement();
                    }
            );
            ThreadUtils.sleepQuietly(200 + new Random().nextInt(500)); // introduce a bit of processing delay - to make sure polling backpressure kicks in.
            inProcess.put(key, new AtomicBoolean(false));
        });
        await().until(() -> partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum() > 1000); // wait until we process some messages to get the counts in.
        Assertions.assertTrue(partitionCounts.values().stream().allMatch(v -> v.get() > 0), "Expect all partitions to have some messages processed, actual partitionCounts:" + partitionCounts);
    }

    @SneakyThrows
    @Test
    void heterogeneousBatch() {
        var numberOfRecordsToProduce = 100L;
        Map<Integer, AtomicInteger> partitionCounts = new HashMap<>();
        IntStream.range(0, 5).forEach(part -> partitionCounts.put(part, new AtomicInteger(0)));
        pc = setupPC(options -> options.messageBufferSize(5000));
        pc.subscribe(UniSets.of(topic));

        getKcu().produceMessages(topic, numberOfRecordsToProduce, "", 50);

        var inProcess = new HashMap<String, AtomicBoolean>();
        pc.poll(recordContexts -> {
            var records = recordContexts.stream().toList();
            var key = records.getFirst().key();
            Assertions.assertTrue(BATCH_SIZE >= records.size(), "Batch size exceeds configured batch limit");
            Assertions.assertFalse(inProcess.getOrDefault(key, new AtomicBoolean(false)).get(), "key is in progress in multiple threads");
            inProcess.put(key, new AtomicBoolean(true));

            recordContexts.stream().forEach(context -> {
                        partitionCounts.get(context.partition()).getAndIncrement();
                    }
            );
            ThreadUtils.sleepQuietly(200 + new Random().nextInt(500)); // introduce a bit of processing delay - to make sure polling backpressure kicks in.
            inProcess.put(key, new AtomicBoolean(false));
        });
        await().until(() -> partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum() > 50); // wait until we process some messages to get the counts in.
        Assertions.assertTrue(partitionCounts.values().stream().allMatch(v -> v.get() > 0), "Expect all partitions to have some messages processed, actual partitionCounts:" + partitionCounts);
    }

    @SneakyThrows
    @Test
    void mixedBatch() {
        var numberOfRecordsToProduce = 100L;
        Map<Integer, AtomicInteger> partitionCounts = new HashMap<>();
        IntStream.range(0, 5).forEach(part -> partitionCounts.put(part, new AtomicInteger(0)));
        pc = setupPC(options -> options.messageBufferSize(5000).batchClassifier(c -> Integer.parseInt(c.key().substring(4)) % 2 == 0));
        pc.subscribe(UniSets.of(topic));

        getKcu().produceMessages(topic, numberOfRecordsToProduce, "", 50);

        var inProcess = new HashMap<String, AtomicBoolean>();
        pc.poll(recordContexts -> {
            System.out.printf("Batch size %d of keys %s%n", recordContexts.size(), recordContexts.stream().map(RecordContext::key).distinct().toList());
            var records = recordContexts.stream().toList();
            var key = records.getFirst().key();
            Assertions.assertTrue(SINGLE_KEY_BATCH_SIZE >= records.size(), String.format("Batch size of %d exceeds configured batch limit of %d", records.size(), SINGLE_KEY_BATCH_SIZE));
            if (Integer.parseInt(key.substring(4)) % 2 == 0) {
                Assertions.assertFalse(inProcess.getOrDefault(key, new AtomicBoolean(false)).get(), String.format("key %s is in progress in multiple threads", key));
            }
            inProcess.put(key, new AtomicBoolean(true));

            recordContexts.stream().forEach(context -> {
                        partitionCounts.get(context.partition()).getAndIncrement();
                    }
            );
            ThreadUtils.sleepQuietly(200 + new Random().nextInt(500)); // introduce a bit of processing delay - to make sure polling backpressure kicks in.
            inProcess.put(key, new AtomicBoolean(false));
        });
        await().until(() -> partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum() > 50); // wait until we process some messages to get the counts in.
        Assertions.assertTrue(partitionCounts.values().stream().allMatch(v -> v.get() > 0), "Expect all partitions to have some messages processed, actual partitionCounts:" + partitionCounts);
    }

    // Tune consumer for smaller message polls both per partition and max poll records.
    private Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 500 * 100); // 500 * ~100 byte message
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // default
        return props;
    }

}
