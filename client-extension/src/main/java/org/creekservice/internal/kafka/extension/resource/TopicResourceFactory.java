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

import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.logging.LoggingField;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;

/** Factory class that builds the {@link Topic} resources */
final class TopicResourceFactory {

    private final ClustersSerdeProviders serdeProviders;
    private final TopicFactory topicFactory;

    /**
     * Constructor.
     *
     * @param serdeProviders all known serde providers.
     */
    TopicResourceFactory(final ClustersSerdeProviders serdeProviders) {
        this(serdeProviders, Topic::new);
    }

    @VisibleForTesting
    TopicResourceFactory(
            final ClustersSerdeProviders serdeProviders, final TopicFactory topicFactory) {
        this.serdeProviders = requireNonNull(serdeProviders, "serdeProviders");
        this.topicFactory = requireNonNull(topicFactory, "topicFactory");
    }

    /**
     * Create a topic resource.
     *
     * @param topic the topic descriptor to create a resource for.
     * @param kafkaProperties the properties of the cluster the {@code topic} belongs to.
     * @return the created resource.
     */
    public <K, V> KafkaTopic<K, V> create(
            final KafkaTopicDescriptor<K, V> topic, final Map<String, ?> kafkaProperties) {
        final Serde<K> keySerde = serde(topic.key(), kafkaProperties);
        final Serde<V> valueSerde = serde(topic.value(), kafkaProperties);
        return topicFactory.create(topic, keySerde, valueSerde);
    }

    private <T> Serde<T> serde(
            final PartDescriptor<T> part, final Map<String, ?> clusterProperties) {
        final KafkaSerdeProvider.SerdeProvider provider = provider(part);
        final Serde<T> serde = provider.createSerde(part);
        serde.configure(clusterProperties, part.part().isKey());
        return serde;
    }

    private <T> KafkaSerdeProvider.SerdeProvider provider(final PartDescriptor<T> part) {
        try {
            return serdeProviders.get(part.format(), part.topic().cluster());
        } catch (final Exception e) {
            throw new UnknownSerializationFormatException(part, e);
        }
    }

    @VisibleForTesting
    interface TopicFactory {
        <K, V> Topic<K, V> create(
                KafkaTopicDescriptor<K, V> def, Serde<K> keySerde, Serde<V> valueSerde);
    }

    @VisibleForTesting
    static final class UnknownSerializationFormatException extends RuntimeException {
        UnknownSerializationFormatException(final PartDescriptor<?> part, final Throwable cause) {
            super(
                    "Unknown "
                            + part.part()
                            + " serialization format encountered."
                            + " format="
                            + part.format()
                            + ", "
                            + LoggingField.topicId
                            + "="
                            + part.topic().id(),
                    cause);
        }
    }
}
