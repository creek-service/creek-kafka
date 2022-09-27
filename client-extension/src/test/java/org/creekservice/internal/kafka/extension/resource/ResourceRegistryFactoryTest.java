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

package org.creekservice.internal.kafka.extension.resource;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ResourceRegistryFactoryTest {

    private static final SerializationFormat KEY_FORMAT = serializationFormat("key-format");
    private static final SerializationFormat VALUE_FORMAT = serializationFormat("value-format");
    private static final Map<String, Object> SOME_CONFIG = Map.of("some", "config");
    private static final String CLUSTER_NAME = "bob";

    @Mock private Collection<? extends ComponentDescriptor> components;
    @Mock private TopicCollector topicCollector;
    @Mock private ResourceRegistryFactory.RegistryFactory registryFactory;
    @Mock private ResourceRegistryFactory.TopicFactory topicFactory;
    @Mock private ClustersProperties clusterProperties;
    @Mock private KafkaSerdeProviders serdeProviders;
    @Mock private KafkaSerdeProvider keySerdeProvider;
    @Mock private KafkaSerdeProvider valueSerdeProvider;
    @Mock private Serde<?> keySerde;
    @Mock private Serde<?> valueSerde;
    @Mock private ResourceRegistry registry;
    @Mock private Topic<String, Long> topicOne;
    @Mock private Topic<String, Long> topicTwo;
    @Mock private KafkaTopicDescriptor<String, Long> topicDefA;
    @Mock private KafkaTopicDescriptor<String, Long> topicDefB;
    @Mock private PartDescriptor<String> aKeyPart;
    @Mock private PartDescriptor<Long> aValuePart;
    @Mock private CustomPart<Long> customPart;
    @Mock private PartDescriptor<String> bKeyPart;
    @Mock private PartDescriptor<Long> bValuePart;
    private ResourceRegistryFactory factory;

    @SuppressWarnings({"rawtypes", "unchecked"})
    @BeforeEach
    void setUp() {
        factory =
                new ResourceRegistryFactory(
                        serdeProviders, topicCollector, registryFactory, topicFactory);

        when(registryFactory.create(any())).thenReturn(registry);

        when(serdeProviders.get(KEY_FORMAT)).thenReturn(keySerdeProvider);
        when(serdeProviders.get(VALUE_FORMAT)).thenReturn(valueSerdeProvider);

        when(keySerdeProvider.create(any())).thenReturn((Serde) keySerde);
        when(valueSerdeProvider.create(any())).thenReturn((Serde) valueSerde);

        setUpPart(aKeyPart, String.class, KEY_FORMAT);
        setUpPart(bKeyPart, String.class, KEY_FORMAT);
        setUpPart(aValuePart, long.class, VALUE_FORMAT);
        setUpPart(bValuePart, long.class, VALUE_FORMAT);
        setUpPart(customPart, long.class, VALUE_FORMAT);

        setUpTopic(topicDefA, "a", aKeyPart, aValuePart);
        setUpTopic(topicDefB, "b", bKeyPart, bValuePart);

        when(topicFactory.create(any(), any(), any())).thenReturn((Topic) topicOne);

        when(topicCollector.collectTopics(any()))
                .thenReturn(Map.of(URI.create("topic://default/A"), topicDefA));
    }

    @Test
    void shouldCollectTopicDescriptors() {
        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(topicCollector).collectTopics(components);
    }

    @Test
    void shouldNotBlowUpIfNoKafkaResources() {
        // Given:
        when(topicCollector.collectTopics(any())).thenReturn(Map.of());

        // When:
        final ResourceRegistry result = factory.create(components, clusterProperties);

        // Then:
        verify(registryFactory).create(Map.of());
        assertThat(result, is(registry));
    }

    @Test
    void shouldGetKeySerdeFromProviders() {
        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(serdeProviders).get(topicDefA.key().format());
        verify(keySerdeProvider).create(topicDefA.key());
        verify(topicFactory).create(any(), eq(keySerde), any());
    }

    @Test
    void shouldGetValueSerdeFromProviders() {
        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(serdeProviders).get(topicDefA.value().format());
        verify(valueSerdeProvider).create(topicDefA.value());
        verify(topicFactory).create(any(), any(), eq(valueSerde));
    }

    @Test
    void shouldSupportPassingPartDescriptorSubtyping() {
        // Given:
        when(topicDefA.value()).thenReturn(customPart);

        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(valueSerdeProvider).create(customPart);
    }

    @Test
    void shouldConfigureKeySerde() {
        // Given:
        when(clusterProperties.get(CLUSTER_NAME)).thenReturn(SOME_CONFIG);

        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(keySerde).configure(SOME_CONFIG, true);
    }

    @Test
    void shouldConfigureValueSerde() {
        // Given:
        when(clusterProperties.get(CLUSTER_NAME)).thenReturn(SOME_CONFIG);

        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(valueSerde).configure(SOME_CONFIG, false);
    }

    @Test
    void shouldCreateTopicsWithCorrectParams() {
        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(topicFactory).create(eq(topicDefA), any(), any());
    }

    @Test
    void shouldCreateTopicResourcesForEachTopicDescriptor() {
        // Given:
        when(topicCollector.collectTopics(any()))
                .thenReturn(
                        Map.of(
                                URI.create("topic://default/a"),
                                topicDefA,
                                URI.create("topic://default/b"),
                                topicDefB));
        when(topicFactory.create(eq(topicDefB), any(), any())).thenReturn(topicTwo);

        // When:
        factory.create(components, clusterProperties);

        // Then:
        verify(registryFactory)
                .create(
                        Map.of(
                                URI.create("topic://default/a"),
                                topicOne,
                                URI.create("topic://default/b"),
                                topicTwo));
    }

    @Test
    void shouldThrowOnUnknownKeyFormat() {
        // Given:
        final RuntimeException cause = new RuntimeException("Boom");
        when(serdeProviders.get(KEY_FORMAT)).thenThrow(cause);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> factory.create(components, clusterProperties));

        // Then:
        assertThat(
                e.getMessage(),
                is("Unknown key serialization format encountered. format=key-format, topic=a"));

        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowOnUnknownValueFormat() {
        // Given:
        final RuntimeException cause = new RuntimeException("Boom");
        when(serdeProviders.get(VALUE_FORMAT)).thenThrow(cause);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> factory.create(components, clusterProperties));

        // Then:
        assertThat(
                e.getMessage(),
                is("Unknown value serialization format encountered. format=value-format, topic=a"));

        assertThat(e.getCause(), is(cause));
    }

    private <T> void setUpPart(
            final PartDescriptor<T> part,
            final Class<T> keyType,
            final SerializationFormat format) {
        when(part.type()).thenReturn(keyType);
        when(part.format()).thenReturn(format);
    }

    private <K, V> void setUpTopic(
            final KafkaTopicDescriptor<K, V> topic,
            final String name,
            final PartDescriptor<K> keyPart,
            final PartDescriptor<V> valuePart) {
        when(topic.name()).thenReturn(name);
        when(topic.cluster()).thenReturn(CLUSTER_NAME);
        when(topic.key()).thenReturn(keyPart);
        when(topic.value()).thenReturn(valuePart);
    }

    private interface CustomPart<T> extends PartDescriptor<T> {}
}
