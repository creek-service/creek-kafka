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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.KafkaTopicOutput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.test.env.listener.TestEnvironmentListener;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.test.model.TestSuiteResult;
import org.creekservice.internal.kafka.extension.resource.TopicCollector;
import org.creekservice.internal.kafka.extension.resource.TopicCollector.CollectedTopics;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicValidator;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TopicValidatingListener implements TestEnvironmentListener, TopicValidator {

    private final CreekSystemTest api;
    private final TopicCollector topicCollector;
    private Optional<CollectedTopics> collectedTopics = Optional.empty();

    public TopicValidatingListener(final CreekSystemTest api) {
        this(api, new TopicCollector());
    }

    @VisibleForTesting
    TopicValidatingListener(final CreekSystemTest api, final TopicCollector topicCollector) {
        this.api = requireNonNull(api, "api");
        this.topicCollector = requireNonNull(topicCollector, "topicCollector");
    }

    @Override
    public void beforeSuite(final CreekTestSuite suite) {
        final List<? extends ServiceDescriptor> servicesUnderTest =
                api.tests().env().currentSuite().services().stream()
                        .map(ServiceInstance::descriptor)
                        .flatMap(Optional::stream)
                        .collect(Collectors.toList());

        collectedTopics = Optional.of(topicCollector.collectTopics(servicesUnderTest));
    }

    @Override
    public void afterSuite(final CreekTestSuite suite, final TestSuiteResult result) {
        collectedTopics = Optional.empty();
    }

    public void validateCanProduce(final KafkaTopic<?, ?> topic) {
        if (allDescriptorsAre(
                d -> d instanceof KafkaTopicOutput | d instanceof OwnedKafkaTopicOutput, topic)) {
            throw new InvalidTopicOperationException("produce to", "consume from", topic);
        }
    }

    public void validateCanConsume(final KafkaTopic<?, ?> topic) {
        if (allDescriptorsAre(
                d -> d instanceof KafkaTopicInput | d instanceof OwnedKafkaTopicInput, topic)) {
            throw new InvalidTopicOperationException("consume from", "produce to", topic);
        }
    }

    private boolean allDescriptorsAre(
            final Predicate<KafkaTopicDescriptor<?, ?>> predicate, final KafkaTopic<?, ?> topic) {

        final List<KafkaTopicDescriptor<?, ?>> allDescriptors =
                collectedTopics
                        .orElseThrow(IllegalStateException::new)
                        .getAll(topic.descriptor().id());

        return allDescriptors.stream().allMatch(predicate);
    }

    private static final class InvalidTopicOperationException extends RuntimeException {
        InvalidTopicOperationException(
                final String op, final String reverseOp, final KafkaTopic<?, ?> topic) {
            super(
                    "Tests can not "
                            + op
                            + " topic "
                            + topic.descriptor().id()
                            + " as the services-under-test do not "
                            + reverseOp
                            + " it. Please check the topic name.");
        }
    }
}