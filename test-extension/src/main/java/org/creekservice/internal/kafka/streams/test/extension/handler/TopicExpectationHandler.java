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

import java.net.URI;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;

/** {@link ExpectationHandler} for {@link TopicExpectation} */
public final class TopicExpectationHandler implements ExpectationHandler<TopicExpectation> {

    private final ClientsExtension clientsExt;
    private final SystemTestSerdeProviders testSerdeProviders;
    private final RecordNormaliser recordNormaliser = new RecordNormaliser();
    private final TopicValidator topicValidator;

    /**
     * @param clientsExt client test extension
     * @param testSerdeProviders the system test serde providers.
     * @param topicValidator topic validator
     */
    public TopicExpectationHandler(
            final ClientsExtension clientsExt,
            final SystemTestSerdeProviders testSerdeProviders,
            final TopicValidator topicValidator) {
        this.clientsExt = requireNonNull(clientsExt, "clientsExt");
        this.testSerdeProviders = requireNonNull(testSerdeProviders, "testSerdeProviders");
        this.topicValidator = requireNonNull(topicValidator, "topicValidator");
    }

    @Override
    public Verifier prepare(
            final Collection<? extends TopicExpectation> expectations,
            final ExpectationOptions options) {
        final Map<String, Map<String, List<TopicRecord>>> byClusterThenTopic =
                expectations.stream()
                        .map(TopicExpectation::records)
                        .flatMap(List::stream)
                        .collect(
                                groupingBy(
                                        TopicRecord::clusterName,
                                        LinkedHashMap::new,
                                        groupingBy(
                                                TopicRecord::topicName,
                                                LinkedHashMap::new,
                                                toList())));

        final List<Verifier> clusterVerifiers =
                byClusterThenTopic.entrySet().stream()
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
                new TopicConsumers(testTopics, clientsExt.consumer(cluster));

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
}
