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

package org.creekservice.internal.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;

/** Kafka streams Creek extension. */
public final class StreamsExtension implements KafkaStreamsExtension {

    static final String NAME = "org.creekservice.kafka.streams";

    private final KafkaClientsExtension clientsExtension;
    private final KafkaStreamsBuilder appBuilder;
    private final KafkaStreamsExecutor appExecutor;

    /**
     * @param clientsExtension the client extension/
     * @param appBuilder the streams app builder to use.
     * @param appExecutor the streams app executor to use.
     */
    public StreamsExtension(
            final KafkaClientsExtension clientsExtension,
            final KafkaStreamsBuilder appBuilder,
            final KafkaStreamsExecutor appExecutor) {
        this.clientsExtension = requireNonNull(clientsExtension, "clientsExtension");
        this.appBuilder = requireNonNull(appBuilder, "appBuilder");
        this.appExecutor = requireNonNull(appExecutor, "appExecutor");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Properties properties(final String clusterName) {
        return clientsExtension.properties(clusterName);
    }

    @Override
    public <K, V> KafkaTopic<K, V> topic(final KafkaTopicDescriptor<K, V> def) {
        return clientsExtension.topic(def);
    }

    @Override
    public Producer<byte[], byte[]> producer(final String clusterName) {
        return clientsExtension.producer(clusterName);
    }

    @Override
    public Consumer<byte[], byte[]> consumer(final String clusterName) {
        return clientsExtension.consumer(clusterName);
    }

    @Override
    public void close(final Duration timeout) {
        clientsExtension.close(timeout);
    }

    @Override
    public KafkaStreams build(final Topology topology, final String clusterName) {
        return appBuilder.build(topology, clusterName);
    }

    @Override
    public void execute(final KafkaStreams app) {
        appExecutor.execute(app);
    }
}
