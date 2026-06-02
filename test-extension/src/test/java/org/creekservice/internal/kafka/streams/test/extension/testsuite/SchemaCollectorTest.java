/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.schema.SchemaDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.SchemaCollector.CollectedSchemaRegistries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SchemaCollectorTest {

    @Mock private ComponentDescriptor component;
    @Mock private ComponentDescriptor component2;
    @Mock private KafkaTopicDescriptor<String, Long> topic;
    @Mock private KafkaTopicDescriptor<String, Long> topic2;
    @Mock private KafkaTopicDescriptor.PartDescriptor<String> keyPart;
    @Mock private KafkaTopicDescriptor.PartDescriptor<Long> valuePart;
    @Mock private KafkaTopicDescriptor.PartDescriptor<String> keyPart2;
    @Mock private KafkaTopicDescriptor.PartDescriptor<Long> valuePart2;
    @Mock private SchemaDescriptor<?> schema;
    @Mock private SchemaDescriptor<?> schema2;
    @Mock private ResourceDescriptor nonKafkaResource;

    private final SchemaCollector collector = new SchemaCollector();

    @BeforeEach
    void setUp() {
        doReturn(Stream.of(topic)).when(component).resources();
        doReturn(Stream.of(topic2)).when(component2).resources();

        when(topic.cluster()).thenReturn("test-cluster-a");
        when(topic2.cluster()).thenReturn("test-cluster-b");

        doReturn(Stream.of(schema)).when(topic).resources();
        doReturn(Stream.of(schema2)).when(topic2).resources();

        when(schema.schemaRegistryName()).thenReturn("registry-a");
        when(schema2.schemaRegistryName()).thenReturn("registry-b");

        doReturn(keyPart).when(schema).part();
        doReturn(valuePart2).when(schema2).part();
        doReturn(topic).when(keyPart).topic();
        doReturn(topic2).when(valuePart2).topic();
    }

    @Test
    void shouldReturnEmptyWhenNoComponents() {
        // When:
        final CollectedSchemaRegistries result = collector.collectSchemaRegistries(List.of());

        // Then:
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void shouldReturnEmptyWhenNoSchemaResources() {
        // Given:
        doReturn(Stream.of(nonKafkaResource)).when(component).resources();

        // When:
        final CollectedSchemaRegistries result =
                collector.collectSchemaRegistries(List.of(component));

        // Then:
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void shouldCollectSchemaRegistries() {
        // When:
        final CollectedSchemaRegistries result =
                collector.collectSchemaRegistries(List.of(component, component2));

        // Then:
        assertThat(result.registryNames(), is(Set.of("registry-a", "registry-b")));
        assertThat(result.clusterFor("registry-a"), is("test-cluster-a"));
        assertThat(result.clusterFor("registry-b"), is("test-cluster-b"));
    }

    @Test
    void shouldDeduplicateRegistries() {
        // Given:
        when(topic2.cluster()).thenReturn("test-cluster-a");
        when(schema2.schemaRegistryName()).thenReturn("registry-a");

        // When:
        final CollectedSchemaRegistries result =
                collector.collectSchemaRegistries(List.of(component, component2));

        // Then:
        assertThat(result.registryNames(), containsInAnyOrder("registry-a"));
        assertThat(result.clusterFor("registry-a"), is("test-cluster-a"));
    }

    @Test
    void shouldThrowWhenRegistryAssociatedWithMultipleClusters() {
        // Given:
        when(schema2.schemaRegistryName()).thenReturn("registry-a");

        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> collector.collectSchemaRegistries(List.of(component, component2)));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Schema registry 'registry-a' is associated with multiple Kafka clusters:"
                                + " 'test-cluster-a' and 'test-cluster-b'"));
    }

    @Test
    void shouldThrowForUnknownRegistry() {
        // Given:
        final CollectedSchemaRegistries result =
                collector.collectSchemaRegistries(List.of(component));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> result.clusterFor("unknown-registry"));

        // Then:
        assertThat(e.getMessage(), containsString("Unknown schema registry: 'unknown-registry'"));
    }
}
