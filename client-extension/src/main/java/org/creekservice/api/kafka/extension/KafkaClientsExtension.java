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

package org.creekservice.api.kafka.extension;

import java.io.Closeable;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.service.extension.CreekExtension;

/** Kafka client extension to Creek. */
public interface KafkaClientsExtension extends CreekExtension, Closeable {

    /**
     * Get the Kafka properties that should be used to build a topology.
     *
     * <p>Note: the properties should be considered immutable. Changing them may result in undefined
     * behaviour.
     *
     * @param clusterName the name of the Kafka cluster to get client properties for. Often will be
     *     {@link KafkaTopicDescriptor#DEFAULT_CLUSTER_NAME}.
     * @return the properties.
     */
    Properties properties(String clusterName);

    /**
     * Get a topic resource for the supplied {@code def}.
     *
     * @param def the topic descriptor
     * @param <K> the key type
     * @param <V> the value type
     * @return the topic resource.
     */
    <K, V> KafkaTopic<K, V> topic(KafkaTopicDescriptor<K, V> def);

    /**
     * Get the shared producer for a cluster.
     *
     * <p>Using a shared binary producer is more efficient that creating different producers, with
     * different serializers, for different topics.
     *
     * <p>The {@link KafkaTopic#serializeKey} and {@link KafkaTopic#serializeValue} convenience
     * methods can be used to serialize the key and value for use with this binary producer.
     *
     * @param clusterName the name of the cluster.
     * @return the shared producer.
     */
    Producer<byte[], byte[]> producer(String clusterName);

    /**
     * Convenience method for getting the shared producer for the default cluster.
     *
     * @return the producer.
     * @see #producer(String)
     */
    default Producer<byte[], byte[]> producer() {
        return producer(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME);
    }

    /**
     * Get the shared consumer for a cluster.
     *
     * <p>Using a shared binary consumer is more efficient that creating different consumers, with
     * different deserializers, for different topics.
     *
     * <p>The {@link KafkaTopic#deserializeKey} and {@link KafkaTopic#deserializeValue} convenience
     * methods can be used to deserialize the keys and values returned by this binary consumer.
     *
     * @param clusterName the name of the cluster.
     * @return the shared consumer.
     */
    Consumer<byte[], byte[]> consumer(String clusterName);

    /**
     * Convenience method for getting the shared consumer for the default cluster.
     *
     * @return the consumer.
     * @see #consumer(String)
     */
    default Consumer<byte[], byte[]> consumer() {
        return consumer(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME);
    }

    /**
     * Close the resources associated with the extension.
     *
     * <p>Closes all shared {@link #producer(String) producers} and {@link #consumer(String)
     * consumers}, waiting for up to the supplier {@code timeout} for <i>each</i> to close.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests.
     * @see org.apache.kafka.clients.producer.KafkaProducer#close(Duration)
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#close(Duration)
     */
    void close(Duration timeout);

    /**
     * Close the resources associated with the extension.
     *
     * <p>Closes all shared {@link #producer(String) producers} and {@link #consumer(String)}
     * consumers}, waiting until all previously sent requests complete.
     *
     * @see org.apache.kafka.clients.producer.KafkaProducer#close(Duration)
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#close(Duration)
     */
    default void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }
}
