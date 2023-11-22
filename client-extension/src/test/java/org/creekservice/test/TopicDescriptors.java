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

package org.creekservice.test;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;

import java.net.URI;
import java.util.Optional;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopicInternal;
import org.creekservice.api.kafka.metadata.KafkaTopicConfig;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.metadata.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.KafkaTopicInternal;
import org.creekservice.api.kafka.metadata.KafkaTopicOutput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.NativeKafkaSerde;

public final class TopicDescriptors {

    public static final SerializationFormat KAFKA_FORMAT = serializationFormat("kafka");
    public static final SerializationFormat OTHER_FORMAT = serializationFormat("other");

    private TopicDescriptors() {}

    public static <K, V> OwnedKafkaTopicInput<K, V> inputTopic(
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return inputTopic(DEFAULT_CLUSTER_NAME, topicName, keyType, valueType, config);
    }

    public static <K, V> OwnedKafkaTopicInput<K, V> inputTopic(
            final String clusterName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return new InputTopicDescriptor<>(clusterName, topicName, keyType, valueType, config);
    }

    public static <K, V> KafkaTopicInternal<K, V> internalTopic(
            final String topicName, final Class<K> keyType, final Class<V> valueType) {
        return internalTopic(DEFAULT_CLUSTER_NAME, topicName, keyType, valueType);
    }

    public static <K, V> KafkaTopicInternal<K, V> internalTopic(
            final String clusterName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType) {
        return new InternalTopicDescriptor<>(clusterName, topicName, keyType, valueType);
    }

    public static <K, V> CreatableKafkaTopicInternal<K, V> creatableInternalTopic(
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return creatableInternalTopic(DEFAULT_CLUSTER_NAME, topicName, keyType, valueType, config);
    }

    public static <K, V> CreatableKafkaTopicInternal<K, V> creatableInternalTopic(
            final String clusterName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return new CreatableInternalTopicDescriptor<>(
                clusterName, topicName, keyType, valueType, config);
    }

    public static <K, V> OwnedKafkaTopicOutput<K, V> outputTopic(
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return outputTopic(DEFAULT_CLUSTER_NAME, topicName, keyType, valueType, config);
    }

    public static <K, V> OwnedKafkaTopicOutput<K, V> outputTopic(
            final String clusterName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return new OutputTopicDescriptor<>(clusterName, topicName, keyType, valueType, config);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private abstract static class TopicDescriptor<K, V> implements KafkaTopicDescriptor<K, V> {
        private final String topicName;
        private final String clusterName;
        private final PartDescriptor<K> key;
        private final PartDescriptor<V> value;
        private final Optional<KafkaTopicConfig> config;

        TopicDescriptor(
                final String clusterName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final Optional<TopicConfigBuilder> config) {
            this.clusterName = requireNonBlank(clusterName, "clusterName");
            this.topicName = requireNonBlank(topicName, "topicName");
            this.key = partDescriptor(Part.key, keyType);
            this.value = partDescriptor(Part.value, valueType);
            this.config = requireNonNull(config, "config").map(TopicConfigBuilder::build);
        }

        @Override
        public String cluster() {
            return clusterName;
        }

        @Override
        public String name() {
            return topicName;
        }

        @Override
        public PartDescriptor<K> key() {
            return key;
        }

        @Override
        public PartDescriptor<V> value() {
            return value;
        }

        public KafkaTopicConfig config() {
            return config.orElseThrow();
        }

        private <T> PartDescriptor<T> partDescriptor(final Part part, final Class<T> partType) {
            return NativeKafkaSerde.supports(partType)
                    ? new KafkaPart<>(part, partType)
                    : new OtherPart<>(part, partType);
        }

        private final class KafkaPart<T> implements PartDescriptor<T> {

            private final Part part;
            private final Class<T> type;

            KafkaPart(final Part part, final Class<T> type) {
                this.part = requireNonNull(part, "part");
                this.type = requireNonNull(type, "type");
            }

            @Override
            public Part part() {
                return part;
            }

            @Override
            public SerializationFormat format() {
                return KAFKA_FORMAT;
            }

            @Override
            public Class<T> type() {
                return type;
            }

            @Override
            public KafkaTopicDescriptor<?, ?> topic() {
                return TopicDescriptor.this;
            }
        }

        private final class OtherPart<T> implements PartDescriptor<T> {

            private final Part part;
            private final Class<T> type;

            OtherPart(final Part part, final Class<T> type) {
                this.part = requireNonNull(part, "part");
                this.type = requireNonNull(type, "type");
            }

            @Override
            public Part part() {
                return part;
            }

            @Override
            public SerializationFormat format() {
                return OTHER_FORMAT;
            }

            @Override
            public Class<T> type() {
                return type;
            }

            @Override
            public KafkaTopicDescriptor<?, ?> topic() {
                return TopicDescriptor.this;
            }
        }
    }

    private static final class OutputTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements OwnedKafkaTopicOutput<K, V> {

        OutputTopicDescriptor(
                final String clusterName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final TopicConfigBuilder config) {
            super(clusterName, topicName, keyType, valueType, Optional.of(config));
        }

        @Override
        public KafkaTopicInput<K, V> toInput() {
            final OwnedKafkaTopicOutput<K, V> output = this;
            return new KafkaTopicInput<>() {
                @Override
                public URI id() {
                    return output.id();
                }

                @Override
                public String cluster() {
                    return output.cluster();
                }

                @Override
                public String name() {
                    return output.name();
                }

                @Override
                public PartDescriptor<K> key() {
                    return output.key();
                }

                @Override
                public PartDescriptor<V> value() {
                    return output.value();
                }
            };
        }
    }

    private static final class InputTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements OwnedKafkaTopicInput<K, V> {

        InputTopicDescriptor(
                final String clusterName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final TopicConfigBuilder config) {
            super(clusterName, topicName, keyType, valueType, Optional.of(config));
        }

        @Override
        public KafkaTopicOutput<K, V> toOutput() {
            final OwnedKafkaTopicInput<K, V> input = this;
            return new KafkaTopicOutput<>() {
                @Override
                public URI id() {
                    return input.id();
                }

                @Override
                public String cluster() {
                    return input.cluster();
                }

                @Override
                public String name() {
                    return input.name();
                }

                @Override
                public PartDescriptor<K> key() {
                    return input.key();
                }

                @Override
                public PartDescriptor<V> value() {
                    return input.value();
                }
            };
        }
    }

    private static final class InternalTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements KafkaTopicInternal<K, V> {

        InternalTopicDescriptor(
                final String clusterName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType) {
            super(clusterName, topicName, keyType, valueType, Optional.empty());
        }
    }

    private static final class CreatableInternalTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements CreatableKafkaTopicInternal<K, V> {

        CreatableInternalTopicDescriptor(
                final String clusterName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final TopicConfigBuilder config) {
            super(clusterName, topicName, keyType, valueType, Optional.of(config));
        }
    }
}
