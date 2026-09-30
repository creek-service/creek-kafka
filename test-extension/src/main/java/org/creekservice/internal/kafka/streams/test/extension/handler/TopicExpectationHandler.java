/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.system.test.extension.test.env.listener.TestEnvironmentListener;
import org.creekservice.api.system.test.extension.test.model.CreekTestCase;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler;
import org.creekservice.api.system.test.extension.test.model.TestCaseResult;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;

/** {@link ExpectationHandler} for {@link TopicExpectation} */
public final class TopicExpectationHandler
        implements ExpectationHandler<TopicExpectation>, TestEnvironmentListener {

    private final ClientsExtension clientsExt;
    private final SystemTestSerdeProviders testSerdeProviders;
    private final RecordNormaliser recordNormaliser = new RecordNormaliser();
    private final TopicValidator topicValidator;
    private final TopicConsumersFactory topicConsumersFactory;
    private final Map<String, SeedOffsetOverrides> clusterToSeedOffsets = new HashMap<>();

    /**
     * @param clientsExt Kafka clients extension
     * @param testSerdeProviders system-test serde providers
     * @param topicValidator validates expected topics
     */
    public TopicExpectationHandler(
            final ClientsExtension clientsExt,
            final SystemTestSerdeProviders testSerdeProviders,
            final TopicValidator topicValidator) {
        this(clientsExt, testSerdeProviders, topicValidator, TopicConsumers::new);
    }

    @VisibleForTesting
    TopicExpectationHandler(
            final ClientsExtension clientsExt,
            final SystemTestSerdeProviders testSerdeProviders,
            final TopicValidator topicValidator,
            final TopicConsumersFactory topicConsumersFactory) {
        this.clientsExt = requireNonNull(clientsExt, "clientsExt");
        this.testSerdeProviders = requireNonNull(testSerdeProviders, "testSerdeProviders");
        this.topicValidator = requireNonNull(topicValidator, "topicValidator");
        this.topicConsumersFactory = requireNonNull(topicConsumersFactory, "topicConsumersFactory");
    }

    @Override
    public void afterSeeding(final CreekTestSuite suite) {
        if (suite.seedData().isEmpty() || suite.tests().isEmpty()) {
            return;
        }

        final CreekTestCase test = suite.tests().get(0);

        final Map<String, Set<String>> topicsWithExpectationsByCluster =
                byClusterThenTopic(
                        test.expectations().stream()
                                .filter(TopicExpectation.class::isInstance)
                                .map(TopicExpectation.class::cast),
                        Collectors.mapping(TopicRecord::topicName, toSet()));

        topicsWithExpectationsByCluster.forEach(this::captureOffsets);
    }

    @Override
    public void afterTest(final CreekTestCase test, final TestCaseResult result) {
        clusterToSeedOffsets.clear();
    }

    @Override
    public Verifier prepare(
            final Collection<? extends TopicExpectation> expectations,
            final ExpectationOptions options) {

        final Map<String, Map<String, List<TopicRecord>>> topicsWithExpectationsByCluster =
                byClusterThenTopic(
                        expectations.stream(),
                        groupingBy(TopicRecord::topicName, LinkedHashMap::new, toList()));

        final List<Verifier> clusterVerifiers =
                topicsWithExpectationsByCluster.entrySet().stream()
                        .map(e -> prepare(e.getKey(), e.getValue(), options))
                        .toList();

        return () -> clusterVerifiers.forEach(Verifier::verify);
    }

    private Verifier prepare(
            final String cluster,
            final Map<String, List<TopicRecord>> byTopic,
            final ExpectationOptions options) {
        final Map<String, KafkaTopic<?, ?>> topics =
                byTopic.entrySet().stream()
                        .collect(
                                toMap(
                                        Map.Entry::getKey,
                                        e ->
                                                kafkaTopic(
                                                        cluster,
                                                        e.getKey(),
                                                        e.getValue().get(0).location())));

        topics.values().forEach(topicValidator::validateCanConsume);

        final Map<String, TestKafkaTopic> testTopics =
                topics.entrySet().stream()
                        .collect(
                                toMap(
                                        Map.Entry::getKey,
                                        e -> testSerdeProviders.get(e.getValue().descriptor())));

        final TopicConsumers topicConsumers =
                topicConsumersFactory.create(
                        testTopics,
                        clientsExt.consumer(cluster),
                        clusterToSeedOffsets.getOrDefault(cluster, new SeedOffsetOverrides()));

        final List<? extends Verifier> topicVerifiers =
                byTopic.entrySet().stream()
                        .map(
                                e ->
                                        topicVerifier(
                                                e.getKey(),
                                                e.getValue(),
                                                options,
                                                testTopics,
                                                topicConsumers))
                        .toList();

        return () -> topicVerifiers.forEach(Verifier::verify);
    }

    private TopicVerifier topicVerifier(
            final String topicName,
            final List<TopicRecord> expectedRecords,
            final ExpectationOptions options,
            final Map<String, TestKafkaTopic> testTopics,
            final TopicConsumers topicConsumers) {

        final TestKafkaTopic testTopic = testTopics.get(topicName);
        final List<TopicRecord> normalisedExpected =
                recordNormaliser.normalise(expectedRecords, testTopic);
        final KafkaOptions kafkaOptions = TestOptionsAccessor.get(options);

        return new TopicVerifier(
                topicName,
                topicConsumers,
                new RecordMatcher(normalisedExpected, kafkaOptions.outputOrdering()),
                kafkaOptions.verifierTimeout().orElse(options.timeout()),
                kafkaOptions.extraTimeout());
    }

    private KafkaTopic<?, ?> kafkaTopic(
            final String cluster, final String topic, final URI location) {
        try {
            return clientsExt.topic(cluster, topic);
        } catch (final Exception e) {
            throw new TopicExpectationException(
                    "The expected record's cluster or topic is not known."
                            + " cluster: "
                            + cluster
                            + ", topic: "
                            + topic
                            + ", location: "
                            + location,
                    e);
        }
    }

    private void captureOffsets(final String cluster, final Set<String> expectationTopics) {
        final SeedOffsetOverrides overrides = new SeedOffsetOverrides();
        final Consumer<byte[], byte[]> consumer = clientsExt.consumer(cluster);
        final Map<String, List<PartitionInfo>> existingTopics = consumer.listTopics();

        expectationTopics.forEach(
                expectationTopic -> {
                    final List<PartitionInfo> pis = existingTopics.get(expectationTopic);
                    if (pis == null) {
                        overrides.putZeroOffsets(expectationTopic);
                    } else {
                        overrides.put(
                                expectationTopic, endOffsets(consumer, expectationTopic, pis));
                    }
                });

        clusterToSeedOffsets.put(cluster, overrides);
    }

    private Map<TopicPartition, Long> endOffsets(
            final Consumer<byte[], byte[]> consumer,
            final String expectationTopic,
            final List<PartitionInfo> pis) {
        final Set<TopicPartition> partitions =
                pis.stream()
                        .map(p -> new TopicPartition(expectationTopic, p.partition()))
                        .collect(toSet());

        return consumer.endOffsets(partitions);
    }

    private static <T> Map<String, T> byClusterThenTopic(
            final Stream<? extends TopicExpectation> expectations,
            final Collector<TopicRecord, ?, T> downstream) {
        return expectations
                .map(TopicExpectation::records)
                .flatMap(List::stream)
                .collect(groupingBy(TopicRecord::clusterName, LinkedHashMap::new, downstream));
    }

    @VisibleForTesting
    interface TopicConsumersFactory {

        TopicConsumers create(
                Map<String, TestKafkaTopic> topics,
                Consumer<byte[], byte[]> consumer,
                SeekOffsetOverrides seekOverrides);
    }

    private static final class SeedOffsetOverrides implements SeekOffsetOverrides {

        private final Map<String, Map<TopicPartition, Long>> offsets = new HashMap<>();

        void put(final String expectationTopic, final Map<TopicPartition, Long> seekOffsets) {
            if (seekOffsets.isEmpty()) {
                throw new IllegalArgumentException("Topic has no overrides: " + expectationTopic);
            }
            offsets.put(expectationTopic, seekOffsets);
        }

        void putZeroOffsets(final String expectationTopic) {
            offsets.put(expectationTopic, Map.of());
        }

        @Override
        public boolean hasOverrides(final String topic) {
            return offsets.containsKey(topic);
        }

        @Override
        public Map<TopicPartition, Long> get(final String topic, final int size) {
            final Map<TopicPartition, Long> override = offsets.get(topic);
            if (override == null) {
                throw new IllegalArgumentException("Topic has no overrides: " + topic);
            }

            return override.isEmpty() ? buildZeroOffset(topic, size) : override;
        }

        private Map<TopicPartition, Long> buildZeroOffset(final String topic, final int size) {
            return IntStream.range(0, size)
                    .mapToObj(idx -> new TopicPartition(topic, idx))
                    .collect(toMap(Function.identity(), tp -> 0L));
        }

        void clear() {
            offsets.clear();
        }
    }
}
