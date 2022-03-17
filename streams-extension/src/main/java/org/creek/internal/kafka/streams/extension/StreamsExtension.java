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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creek.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creek.api.kafka.streams.extension.KafkaStreamsExtensionOptions;

/** Kafka streams Creek extension. */
final class StreamsExtension implements KafkaStreamsExtension {

    static final String NAME = "Kafka-streams";

    private final KafkaStreamsExtensionOptions options;
    private final KafkaStreamsBuilder appBuilder;
    private final KafkaStreamsExecutor appExecutor;

    StreamsExtension(
            final KafkaStreamsExtensionOptions options,
            final KafkaStreamsBuilder appBuilder,
            final KafkaStreamsExecutor appExecutor) {
        this.options = requireNonNull(options, "options");
        this.appBuilder = requireNonNull(appBuilder, "appBuilder");
        this.appExecutor = requireNonNull(appExecutor, "appExecutor");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Properties properties() {
        return options.properties();
    }

    @Override
    public KafkaStreams build(final Topology topology) {
        return appBuilder.build(topology);
    }

    @Override
    public void execute(final KafkaStreams app) {
        appExecutor.execute(app);
    }
}
