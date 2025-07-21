/*
 * Copyright 2025 Creek Contributors (https://github.com/creek-service)
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

package com.acme.examples.streams;

import com.acme.examples.service.MyServiceDescriptor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.kafka.streams.extension.util.Name;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;

import java.time.Duration;

import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;

// begin-snippet: service-main
@SuppressWarnings("unused")
public final class ServiceMain {

    private static final Name name = Name.root();

    public static void main(String... args) {
        // Initialize Creek in the main application entry point:
        CreekContext ctx = CreekServices.context(new MyServiceDescriptor());

        // Access the Kafka Streams extension:
        final KafkaStreamsExtension ext = ctx.extension(KafkaStreamsExtension.class);
        // Use it to help build the topology:
        final Topology topology = new TopologyBuilder(ext).build();
        // And execute it:
        ext.execute(topology);
    }
    // end-snippet

    public static void extensionOptions(String... args) {
        // begin-snippet: extension-options
        // Initialize Creek in the main application entry point:
        CreekContext ctx = CreekServices.builder(new MyServiceDescriptor())
                // Optionally, override default extension options:
                .with(
                        KafkaStreamsExtensionOptions.builder()
                                .withKafkaProperty(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, 200L)
                                .withMetricsPublishing(
                                        KafkaMetricsPublisherOptions.builder()
                                                .withPublishPeriod(Duration.ofMinutes(5))
                                )
                                .build()
                )
                .build();
        // end-snippet
    }
}
