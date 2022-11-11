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

package org.creekservice.internal.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import java.util.Properties;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;

/** Builds a {@link KafkaStreams} app from a {@link Topology}. */
public final class KafkaStreamsBuilder {

    private final KafkaClientsExtension clientsExtension;
    private final AppFactory appFactory;

    /**
     * @param clientsExtension the kafka client extension.
     */
    public KafkaStreamsBuilder(final KafkaClientsExtension clientsExtension) {
        this(clientsExtension, KafkaStreams::new);
    }

    @VisibleForTesting
    KafkaStreamsBuilder(final KafkaClientsExtension clientsExtension, final AppFactory appFactory) {
        this.clientsExtension = requireNonNull(clientsExtension, "clientsExtension");
        this.appFactory = requireNonNull(appFactory, "appFactory");
    }

    /**
     * Build the streams app.
     *
     * @param topology the streams topology.
     * @param clusterName the Kafka cluster name the app will target.
     * @return the streams app.
     */
    public KafkaStreams build(final Topology topology, final String clusterName) {
        final Properties properties = clientsExtension.properties(clusterName);
        final KafkaClientSupplier kafkaClientSupplier = new DefaultKafkaClientSupplier();

        return appFactory.create(topology, properties, kafkaClientSupplier);
    }

    @VisibleForTesting
    interface AppFactory {
        KafkaStreams create(Topology topology, Properties properties, KafkaClientSupplier clients);
    }
}
