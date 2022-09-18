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

package org.creekservice.internal.kafka.extension.resource;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;
import org.creekservice.api.platform.metadata.ComponentDescriptor;

public final class ResourceRegistryFactory {

    private final KafkaSerdeProviders serdeProviders;
    private final TopicCollector topicCollector;
    private final RegistryFactory registryFactory;
    private final TopicFactory topicFactory;

    public ResourceRegistryFactory() {
        this(KafkaSerdeProviders.create(), new TopicCollector(), ResourceRegistry::new, Topic::new);
    }

    @VisibleForTesting
    ResourceRegistryFactory(
            final KafkaSerdeProviders serdeProviders,
            final TopicCollector topicCollector,
            final RegistryFactory registryFactory,
            final TopicFactory topicFactory) {
        this.serdeProviders = requireNonNull(serdeProviders, "serdeProviders");
        this.topicCollector = requireNonNull(topicCollector, "topicCollector");
        this.registryFactory = requireNonNull(registryFactory, "registryFactory");
        this.topicFactory = requireNonNull(topicFactory, "topicFactory");
    }

    public ResourceRegistry create(
            final Collection<? extends ComponentDescriptor> components,
            final ClustersProperties properties) {

        final Map<URI, KafkaTopicDescriptor<?, ?>> topicDefs =
                topicCollector.collectTopics(components);

        final Map<URI, Topic<?, ?>> topics =
                topicDefs.entrySet().stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey,
                                        e -> createTopicResource(e.getValue(), properties)));

        return registryFactory.create(topics);
    }

    private <K, V> Topic<K, V> createTopicResource(
            final KafkaTopicDescriptor<K, V> def, final ClustersProperties properties) {
        final Serde<K> keySerde = serde(def.key(), def.name(), def.cluster(), true, properties);
        final Serde<V> valueSerde =
                serde(def.value(), def.name(), def.cluster(), false, properties);
        return topicFactory.create(def, keySerde, valueSerde);
    }

    private <T> Serde<T> serde(
            final KafkaTopicDescriptor.PartDescriptor<T> part,
            final String topicName,
            final String clusterName,
            final boolean isKey,
            final ClustersProperties properties) {
        final KafkaSerdeProvider provider = provider(part, topicName, isKey);

        final Serde<T> serde = provider.create(part);
        serde.configure(properties.get(clusterName), isKey);
        return serde;
    }

    private <T> KafkaSerdeProvider provider(
            final KafkaTopicDescriptor.PartDescriptor<T> part,
            final String topicName,
            final boolean isKey) {
        try {
            return serdeProviders.get(part.format());
        } catch (final Exception e) {
            throw new UnknownSerializationFormatException(part.format(), topicName, isKey, e);
        }
    }

    @VisibleForTesting
    interface TopicFactory {
        <K, V> Topic<K, V> create(
                KafkaTopicDescriptor<K, V> def, Serde<K> keySerde, Serde<V> valueSerde);
    }

    @VisibleForTesting
    interface RegistryFactory {
        ResourceRegistry create(Map<URI, Topic<?, ?>> topics);
    }

    private static final class UnknownSerializationFormatException extends RuntimeException {
        UnknownSerializationFormatException(
                final SerializationFormat format,
                final String topicName,
                final boolean isKey,
                final Throwable cause) {
            super(
                    "Unknown "
                            + (isKey ? "key" : "value")
                            + " serialization format encountered."
                            + " format="
                            + format
                            + ", topic="
                            + topicName,
                    cause);
        }
    }
}
