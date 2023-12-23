/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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
import org.creekservice.api.kafka.extension.config.TypeOverrides;
import org.creekservice.api.kafka.extension.logging.LoggingField;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;
import org.creekservice.api.service.extension.component.model.ResourceHandler;
import org.creekservice.internal.kafka.extension.client.KafkaTopicClient;
import org.creekservice.internal.kafka.extension.client.TopicClient;

/** Resource handle for topics. */
public class TopicResourceHandler implements ResourceHandler<KafkaTopicDescriptor<?, ?>> {

    private final StructuredLogger logger;
    private final TopicRegistrar resources;
    private final ClustersProperties properties;
    private final ClustersSerdeProviders serdeProviders;
    private final TopicClient.Factory topicClientFactory;
    private final TopicResourceFactory topicResourceFactory;
    private final KafkaResourceValidator validator;

    /**
     * @param typeOverrides known type overrides, used to customise functionality.
     * @param resources the resource registry to register topics in.
     * @param properties Kafka properties of all known clusters.
     */
    public TopicResourceHandler(
            final TypeOverrides typeOverrides,
            final TopicRegistrar resources,
            final ClustersProperties properties) {
        this(typeOverrides, properties, resources, new ClustersSerdeProviders(typeOverrides));
    }

    private TopicResourceHandler(
            final TypeOverrides typeOverrides,
            final ClustersProperties properties,
            final TopicRegistrar resources,
            final ClustersSerdeProviders serdeProviders) {
        this(
                typeOverrides.get(TopicClient.Factory.class).orElse(KafkaTopicClient::new),
                properties,
                serdeProviders,
                resources,
                new TopicResourceFactory(serdeProviders),
                new KafkaResourceValidator(),
                StructuredLoggerFactory.internalLogger(KafkaTopicClient.class));
    }

    @VisibleForTesting
    TopicResourceHandler(
            final TopicClient.Factory topicClientFactory,
            final ClustersProperties properties,
            final ClustersSerdeProviders serdeProviders,
            final TopicRegistrar resources,
            final TopicResourceFactory topicResourceFactory,
            final KafkaResourceValidator validator,
            final StructuredLogger logger) {
        this.topicClientFactory = requireNonNull(topicClientFactory, "topicClientFactory");
        this.properties = requireNonNull(properties, "properties");
        this.serdeProviders = requireNonNull(serdeProviders, "serdeProviders");
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
        ensureTopicResources(cluster, topics);
        ensureSerdeResources(cluster, topics);
    }

    private void ensureTopicResources(
            final String cluster, final List<? extends CreatableKafkaTopic<?, ?>> topics) {
        topicClientFactory.create(cluster, properties.get(cluster)).ensureExternalResources(topics);
    }

    private void ensureSerdeResources(
            final String cluster, final List<? extends CreatableKafkaTopic<?, ?>> topics) {

        logger.debug(
                "Ensuring topic resources",
                log ->
                        log.with(
                                LoggingField.topicIds,
                                topics.stream().map(KafkaTopicDescriptor::id).collect(toList())));

        topics.stream()
                .flatMap(KafkaTopicDescriptor::parts)
                .collect(groupingBy(PartDescriptor::format))
                .forEach(
                        (format, parts) ->
                                serdeProviders.get(format, cluster).ensureExternalResources(parts));
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
                        + KafkaTopicDescriptors.asString(topic)
                        + " ("
                        + codeLocation(topic)
                        + ")");
    }
}
