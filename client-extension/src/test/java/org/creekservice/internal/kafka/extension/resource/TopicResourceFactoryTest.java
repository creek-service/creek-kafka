/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.internal.kafka.extension.resource.TopicResourceFactory.UnknownSerializationFormatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicResourceFactoryTest {

    private static final SerializationFormat KEY_FORMAT =
            SerializationFormat.serializationFormat("key");
    private static final SerializationFormat VAL_FORMAT =
            SerializationFormat.serializationFormat("val");

    @Mock private KafkaTopicDescriptor<Long, String> descriptor;
    @Mock private ClustersSerdeProviders serdeProviders;
    @Mock private TopicResourceFactory.TopicFactory topicFactory;
    @Mock private Map<String, ?> kafkaProperties;
    @Mock private KafkaSerdeProvider.SerdeProvider serdeProvider;
    @Mock private KafkaTopicDescriptor.PartDescriptor<Long> key;
    @Mock private KafkaTopicDescriptor.PartDescriptor<String> value;
    @Mock private Serde<Long> keySerde;
    @Mock private Serde<String> valSerde;
    private TopicResourceFactory topicResourceFactory;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @BeforeEach
    void setUp() {
        topicResourceFactory = new TopicResourceFactory(serdeProviders, topicFactory);

        when(descriptor.id()).thenReturn(URI.create("kafka://something"));
        when(descriptor.cluster()).thenReturn("cluster");
        when(descriptor.key()).thenReturn(key);
        when(descriptor.value()).thenReturn(value);
        when(key.format()).thenReturn(KEY_FORMAT);
        when(value.format()).thenReturn(VAL_FORMAT);
        when(key.part()).thenReturn(Part.key);
        when(value.part()).thenReturn(Part.value);
        when(key.topic()).thenReturn((KafkaTopicDescriptor) descriptor);
        when(value.topic()).thenReturn((KafkaTopicDescriptor) descriptor);

        when(serdeProviders.get(any(), any())).thenReturn(serdeProvider);
        when(serdeProvider.createSerde(key)).thenReturn(keySerde);
        when(serdeProvider.createSerde(value)).thenReturn(valSerde);
    }

    @Test
    void shouldThrowOnFailureToInitializeSerdeProvider() {
        // Given:
        final RuntimeException expected = new RuntimeException("Boom");
        when(serdeProviders.get(any(), any())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(
                        UnknownSerializationFormatException.class,
                        () -> topicResourceFactory.create(descriptor, kafkaProperties));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Unknown key serialization format encountered. format=key,"
                                + " topicId=kafka://something"));
        assertThat(e.getCause(), is(expected));
    }

    @Test
    void shouldGetSerdeProviderByFormatAndCluster() {
        // When:
        topicResourceFactory.create(descriptor, kafkaProperties);

        // Then:
        verify(serdeProviders).get(KEY_FORMAT, "cluster");
    }

    @Test
    void shouldInitializeKeySerde() {
        // When:
        topicResourceFactory.create(descriptor, kafkaProperties);

        // Then:
        verify(keySerde).configure(kafkaProperties, true);
    }

    @Test
    void shouldInitializeValueSerde() {
        // When:
        topicResourceFactory.create(descriptor, kafkaProperties);

        // Then:
        verify(valSerde).configure(kafkaProperties, false);
    }

    @Test
    void shouldCreateTopicResource() {
        // When:
        topicResourceFactory.create(descriptor, kafkaProperties);

        // Then:
        verify(topicFactory).create(descriptor, keySerde, valSerde);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldReturnTopicResource() {
        // Given:
        final Topic<Object, Object> topic = mock(Topic.class);
        when(topicFactory.create(any(), any(), any())).thenReturn(topic);

        // When:
        final KafkaTopic<?, ?> result = topicResourceFactory.create(descriptor, kafkaProperties);

        // Then:
        assertThat(result, is(topic));
    }
}
