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

package org.creekservice.internal.kafka.extension;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.extension.resource.TopicRegistry;

/** Kafka Client Creek extension. */
public final class ClientsExtension implements KafkaClientsExtension {

    static final String NAME = "org.creekservice.kafka.clients";

    private final ClustersProperties clustersProperties;
    private final TopicRegistry resources;
    private final Map<String, Producer<byte[], byte[]>> producers = new ConcurrentHashMap<>();
    private final Map<String, Consumer<byte[], byte[]>> consumers = new ConcurrentHashMap<>();

    /**
     * @param clustersProperties the clusters properties.
     * @param resources known resources.
     */
    public ClientsExtension(
            final ClustersProperties clustersProperties, final TopicRegistry resources) {
        this.clustersProperties = requireNonNull(clustersProperties, "clustersProperties");
        this.resources = requireNonNull(resources, "resources");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Properties properties(final String clusterName) {
        return clustersProperties.properties(clusterName);
    }

    @Override
    public <K, V> KafkaTopic<K, V> topic(final KafkaTopicDescriptor<K, V> def) {
        return resources.topic(def);
    }

    /**
     * Get a topic resource for the supplied {@code cluster} and {@code topic} names.
     *
     * @param cluster the cluster name.
     * @param topic the topic name.
     * @return the topic resource.
     */
    public KafkaTopic<?, ?> topic(final String cluster, final String topic) {
        return resources.topic(cluster, topic);
    }

    @Override
    public Producer<byte[], byte[]> producer(final String clusterName) {
        return producers.computeIfAbsent(clusterName, this::createProducer);
    }

    @Override
    public Consumer<byte[], byte[]> consumer(final String clusterName) {
        return consumers.computeIfAbsent(clusterName, this::createConsumer);
    }

    @Override
    public void close(final Duration timeout) {
        for (final Producer<byte[], byte[]> producer : producers.values()) {
            producer.close(timeout);
        }
        producers.clear();

        for (final Consumer<byte[], byte[]> consumer : consumers.values()) {
            consumer.close(timeout);
        }
        consumers.clear();
    }

    private Producer<byte[], byte[]> createProducer(final String clusterName) {
        return new KafkaProducer<>(
                clustersProperties.get(clusterName),
                new ByteArraySerializer(),
                new ByteArraySerializer());
    }

    private KafkaConsumer<byte[], byte[]> createConsumer(final String clusterName) {
        return new KafkaConsumer<>(
                clustersProperties.get(clusterName),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    }
}
