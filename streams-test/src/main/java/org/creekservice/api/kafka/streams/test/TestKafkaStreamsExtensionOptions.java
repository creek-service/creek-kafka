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

package org.creekservice.api.kafka.streams.test;

import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.test.util.Temp;
import org.creekservice.internal.kafka.extension.client.TopicClient;

/**
 * Kafka streams extension options builder for test code.
 *
 * <p>Configures the streams' extension with a minimum set of config to allow tests to run
 *
 * <p>Example usage:
 *
 * <pre>
 * class TopologyTest {
 *     private static CreekContext ctx;
 *
 *     &#64;BeforeAll
 *     public static void classSetup() {
 *         ctx = CreekServices.builder(new TestServiceDescriptor())
 *                 .with(TestKafkaStreamsExtensionOptions.defaults())
 *                 .build();
 *     }
 * }
 * </pre>
 */
public final class TestKafkaStreamsExtensionOptions {

    private TestKafkaStreamsExtensionOptions() {}

    /**
     * @return options builder with base set of test config, allowing for additional customisation.
     */
    public static KafkaStreamsExtensionOptions.Builder builder() {
        return KafkaStreamsExtensionOptions.builder()
                .withKafkaProperty(
                        StreamsConfig.STATE_DIR_CONFIG,
                        Temp.tempDir("ks-state").toAbsolutePath().toString())
                .withTypeOverride(TopicClient.Factory.class, MockTopicClient::new);
    }

    /**
     * @return options with base set of test config.
     */
    public static KafkaStreamsExtensionOptions defaults() {
        return builder().build();
    }

    private static final class MockTopicClient implements TopicClient {

        private MockTopicClient(final String clusterName, final Map<String, ?> kafkaProperties) {}

        @Override
        public void ensureExternalResources(
                final List<? extends CreatableKafkaTopic<?, ?>> topics) {
            // No-op.
        }
    }
}
