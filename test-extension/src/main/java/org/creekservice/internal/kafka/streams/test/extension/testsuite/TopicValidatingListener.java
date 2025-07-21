/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicOutput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.test.env.listener.TestEnvironmentListener;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.test.model.TestSuiteResult;
import org.creekservice.internal.kafka.extension.resource.TopicCollector;
import org.creekservice.internal.kafka.extension.resource.TopicCollector.CollectedTopics;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicValidator;

/**
 * Implementation of {@link TopicValidator}.
 *
 * <p>The validator tracks the valid set of topics for the current running test suite and uses this
 * to validate which topics can be produced to and consumed from.
 *
 * <p>It will throw exceptions if any test is attempting to produce to an output-only, or consume
 * from an input-only, topic. It does this as such actions are nonsensical and generally indicate
 * user error.
 *
 * <p>An output-only topic is a topic which the services-under-test only output too. As no
 * service-under-test consumes the topic, it doesn't make sense to be producing data to the topic as
 * part of the test inputs. Likewise, an input-only topic is one which no service-under-test
 * produces to, and hence it does not make sense for any expectation to consume the topic.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TopicValidatingListener implements TestEnvironmentListener, TopicValidator {

    private final CreekSystemTest api;
    private final TopicCollector topicCollector;
    private Optional<CollectedTopics> collectedTopics = Optional.empty();

    /**
     * @param api the system test api.
     */
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

    @Override
    public void validateCanProduce(final KafkaTopic<?, ?> topic) {
        if (allDescriptorsAre(
                d -> d instanceof KafkaTopicOutput | d instanceof OwnedKafkaTopicOutput, topic)) {
            throw new InvalidTopicOperationException("produce to", "consume from", topic);
        }
    }

    @Override
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
