/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.model.TestOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;

public final class TopicExpectationHandler implements ExpectationHandler<TopicExpectation> {

    private final ClientsExtension clientsExt;
    private final RecordCoercer recordCoercer = new RecordCoercer();

    public TopicExpectationHandler(final ClientsExtension clientsExt) {
        this.clientsExt = requireNonNull(clientsExt, "clientsExt");
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
                        .collect(toList());

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
                                        e -> kafkaTopic(cluster, e.getKey(), e.getValue())));

        final TopicConsumers topicConsumers =
                new TopicConsumers(topics, clientsExt.consumer(cluster));

        final List<Verifier> topicVerifiers =
                byTopic.entrySet().stream()
                        .map(
                                e ->
                                        topicVerifier(
                                                e.getKey(),
                                                e.getValue(),
                                                options,
                                                topics,
                                                topicConsumers))
                        .collect(toList());

        return () -> topicVerifiers.forEach(Verifier::verify);
    }

    private TopicVerifier topicVerifier(
            final String topicName,
            final List<TopicRecord> expectedRecords,
            final ExpectationOptions options,
            final Map<String, KafkaTopic<?, ?>> topics,
            final TopicConsumers topicConsumers) {

        final KafkaTopicDescriptor<?, ?> topic = topics.get(topicName).descriptor();
        final List<TopicRecord> coercedExpected = recordCoercer.coerce(expectedRecords, topic);
        final TestOptions testOptions = TestOptionsAccessor.get(options);

        return new TopicVerifier(
                topicName,
                topicConsumers,
                new RecordMatcher(coercedExpected, testOptions.outputOrdering()),
                testOptions.verifierTimeout().orElse(options.timeout()),
                testOptions.extraTimeout());
    }

    private KafkaTopic<?, ?> kafkaTopic(
            final String cluster, final String topic, final List<TopicRecord> records) {
        try {
            return clientsExt.topic(cluster, topic);
        } catch (final Exception e) {
            throw new TopicExpectationException(
                    "The expected record's cluster or topic is not known."
                            + " cluster: "
                            + records.get(0).clusterName()
                            + ", topic: "
                            + records.get(0).topicName()
                            + ", location: "
                            + records.get(0).location(),
                    e);
        }
    }
}
