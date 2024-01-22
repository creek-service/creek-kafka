/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension.resource;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.creekservice.api.base.type.CodeLocation.codeLocation;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.logging.LoggingField;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.api.service.extension.component.model.ResourceHandler;
import org.creekservice.internal.kafka.extension.client.TopicClient;

/** Resource handle for topics. */
public class TopicResourceHandler implements ResourceHandler<KafkaTopicDescriptor<?, ?>> {

    private final StructuredLogger logger;
    private final TopicRegistrar resources;
    private final ClustersProperties properties;
    private final TopicClient.Factory topicClientFactory;
    private final TopicResourceFactory topicResourceFactory;
    private final KafkaResourceValidator validator;

    /**
     * @param topicClientFactory topic client factory.
     * @param resources the resource registry to register topics in.
     * @param properties Kafka properties of all known clusters.
     * @param serdeProviders Known serde providers.
     */
    public TopicResourceHandler(
            final TopicClient.Factory topicClientFactory,
            final TopicRegistrar resources,
            final ClustersProperties properties,
            final KafkaSerdeProviders serdeProviders) {
        this(
                topicClientFactory,
                properties,
                resources,
                new TopicResourceFactory(serdeProviders),
                new KafkaResourceValidator(),
                StructuredLoggerFactory.internalLogger(TopicResourceHandler.class));
    }

    @VisibleForTesting
    TopicResourceHandler(
            final TopicClient.Factory topicClientFactory,
            final ClustersProperties properties,
            final TopicRegistrar resources,
            final TopicResourceFactory topicResourceFactory,
            final KafkaResourceValidator validator,
            final StructuredLogger logger) {
        this.topicClientFactory = requireNonNull(topicClientFactory, "topicClientFactory");
        this.properties = requireNonNull(properties, "properties");
        this.resources = requireNonNull(resources, "resources");
        this.topicResourceFactory = requireNonNull(topicResourceFactory, "topicFactory");
        this.validator = requireNonNull(validator, "validator");
        this.logger = requireNonNull(logger, "logger");
    }

    @Override
    public void validate(final Collection<? extends KafkaTopicDescriptor<?, ?>> resourceGroup) {
        validator.validateGroup(resourceGroup);
    }

    /**
     * Called to ensure external resources exist for the topic.
     *
     * <p>e.g. ensure the topic itself exists, plus any schemas the key and value need, etc.
     *
     * @param topics the collection of topic to ensure.
     */
    @Override
    public void ensure(final Collection<? extends KafkaTopicDescriptor<?, ?>> topics) {
        topics.stream()
                .map(TopicResourceHandler::toCreatable)
                .collect(groupingBy(KafkaTopicDescriptor::cluster))
                .forEach(this::ensure);
    }

    /**
     * Called to allow the extension to prepare any internal state for working with this topic.
     *
     * <p>e.g. prepare serde for the key and value, etc.
     *
     * @param topics the collection of topics to prepare for.
     */
    @Override
    public void prepare(final Collection<? extends KafkaTopicDescriptor<?, ?>> topics) {
        topics.stream().collect(groupingBy(KafkaTopicDescriptor::cluster)).forEach(this::prepare);
    }

    private void ensure(
            final String cluster, final List<? extends CreatableKafkaTopic<?, ?>> topics) {

        logger.debug(
                "Ensuring topics",
                log ->
                        log.with(
                                LoggingField.topicIds,
                                topics.stream().map(ResourceDescriptor::id).collect(toList())));

        topicClientFactory.create(cluster, properties.get(cluster)).ensureTopicsExist(topics);
    }

    private void prepare(
            final String cluster, final List<? extends KafkaTopicDescriptor<?, ?>> topics) {

        logger.debug(
                "Preparing topics",
                log ->
                        log.with(
                                LoggingField.topicIds,
                                topics.stream().map(KafkaTopicDescriptor::id).collect(toList())));

        final Map<String, Object> kafkaProperties = properties.get(cluster);

        topics.stream()
                .map(topic -> topicResourceFactory.create(topic, kafkaProperties))
                .forEach(resources::register);
    }

    private static CreatableKafkaTopic<?, ?> toCreatable(final KafkaTopicDescriptor<?, ?> topic) {
        if (topic instanceof CreatableKafkaTopic) {
            return (CreatableKafkaTopic<?, ?>) topic;
        }
        throw new IllegalArgumentException(
                "Topic descriptor is not creatable: "
                        + "id: "
                        + topic.id()
                        + ", type: "
                        + topic.getClass().getName()
                        + ", location: "
                        + codeLocation(topic));
    }
}
