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

package org.creek.internal.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import java.util.Properties;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

/** Builds a {@link KafkaStreams} app from a {@link Topology}. */
public final class KafkaStreamsBuilder {

    private final KafkaStreamsExtensionOptions options;

    public KafkaStreamsBuilder(final KafkaStreamsExtensionOptions options) {
        this.options = requireNonNull(options, "options");
    }

    public KafkaStreams build(final Topology topology) {
        final Properties properties = options.properties();
        // Todo(ac): Wire in compacting producer:
        // https://github.com/creek-service/creek-kafka/issues/15
        final KafkaClientSupplier kafkaClientSupplier = new DefaultKafkaClientSupplier();

        return new KafkaStreams(topology, properties, kafkaClientSupplier);
    }
}
