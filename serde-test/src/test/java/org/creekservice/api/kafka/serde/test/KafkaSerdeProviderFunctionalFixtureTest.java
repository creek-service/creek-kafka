/*
 * Copyright 2024-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.serde.test;

import static org.creekservice.internal.kafka.serde.test.util.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.creekservice.api.kafka.extension.ClientsExtensionOptions;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.internal.kafka.serde.test.util.TopicDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaSerdeProviderFunctionalFixtureTest {

    private static final OwnedKafkaTopicInput<Long, String> TEST_TOPIC =
            TopicDescriptors.inputTopic("Bob", Long.class, String.class, withPartitions(1));
    private KafkaSerdeProviderFunctionalFixture tester;

    @BeforeEach
    void setUp() {
        tester = KafkaSerdeProviderFunctionalFixture.tester(List.of(TEST_TOPIC));
    }

    @Test
    void shouldThrowOnClientOptions() {
        // Given:
        final ClientsExtensionOptions clientsOptions = mock();

        // When:
        assertThrows(
                IllegalArgumentException.class, () -> tester.withExtensionOption(clientsOptions));
    }

    @SuppressWarnings("resource")
    @Test
    void shouldThrowOnUnknownCluster() {
        assertThrows(IllegalArgumentException.class, () -> tester.kafkaContainer("unknown"));
    }
}
