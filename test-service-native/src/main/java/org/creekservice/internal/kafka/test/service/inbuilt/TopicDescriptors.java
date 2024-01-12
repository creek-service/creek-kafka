/*
 * Copyright 2021-2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.test.service.inbuilt;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;
import static org.creekservice.api.kafka.metadata.schema.SchemaDescriptor.DEFAULT_SCHEMA_REGISTRY_NAME;
import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.OwnedJsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.SchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.UnownedJsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.serde.JsonSchemaKafkaSerde;
import org.creekservice.api.kafka.metadata.serde.NativeKafkaSerde;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopicInternal;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicConfig;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInternal;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicOutput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.OwnedResource;
import org.creekservice.api.platform.metadata.ResourceDescriptor;

/**
 * Helper for creating topic descriptors.
 *
 * <p>Wondering where the builds are for {@link KafkaTopicInput} or {@link KafkaTopicOutput}? These
 * should only be created by calling {@link OwnedKafkaTopicInput#toOutput()} and {@link
 * OwnedKafkaTopicOutput#toInput()} on an owned topic descriptor, respectively.
 *
 * <p>Supports native {@code kafka} serde and {@code json-schema} serde.
 */
@SuppressWarnings("unused") // What is unused today may be used tomorrow...
public final class TopicDescriptors {

    private TopicDescriptors() {}

    /**
     * Create an input Kafka topic descriptor.
     *
     * <p>Looking for a version that returns {@link KafkaTopicInput}? Get one of those by calling
     * {@link OwnedKafkaTopicOutput#toInput()} on the topic descriptor defined in the upstream
     * component.
     *
     * @param topicName the logic name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param config the config of the topic.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the input topic descriptor.
     */
    public static <K, V> OwnedKafkaTopicInput<K, V> inputTopic(
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return inputTopic(
                DEFAULT_CLUSTER_NAME,
                DEFAULT_SCHEMA_REGISTRY_NAME,
                topicName,
                keyType,
                valueType,
                config);
    }

    /**
     * Create an input Kafka topic descriptor.
     *
     * <p>Looking for a version that returns {@link KafkaTopicInput}? Get one of those by calling
     * {@link OwnedKafkaTopicOutput#toInput()} on the topic descriptor defined in the upstream
     * component.
     *
     * @param clusterName the logic name of the Kafka cluster the topic is in.
     * @param schemaRegistryName the logic name of schema registry the schemas are stored in.
     * @param topicName the name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param config the config of the topic.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the input topic descriptor.
     */
    public static <K, V> OwnedKafkaTopicInput<K, V> inputTopic(
            final String clusterName,
            final String schemaRegistryName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return new OwnedInputTopicDescriptor<>(
                clusterName, schemaRegistryName, topicName, keyType, valueType, config);
    }

    /**
     * Create a Kafka topic descriptor for a topic that is implicitly created.
     *
     * <p>Most internal topics, e.g. Kafka Streams changelog and repartition topics, are implicitly
     * created, and this is the method to use to build a descriptor for them.
     *
     * <p>For an internal topic that you want Creek to create, use {@link #creatableInternalTopic}.
     *
     * @param topicName the name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the internal topic descriptor.
     */
    public static <K, V> KafkaTopicInternal<K, V> internalTopic(
            final String topicName, final Class<K> keyType, final Class<V> valueType) {
        return internalTopic(
                DEFAULT_CLUSTER_NAME, DEFAULT_SCHEMA_REGISTRY_NAME, topicName, keyType, valueType);
    }

    /**
     * Create a Kafka topic descriptor for a topic that is implicitly created.
     *
     * <p>Most internal topics, e.g. Kafka Streams changelog and repartition topics, are implicitly
     * created, and this is the method to use to build a descriptor for them.
     *
     * <p>For an internal topic that you want Creek to create, use {@link #creatableInternalTopic}.
     *
     * @param clusterName the logical name of the Kafka cluster the topic is in.
     * @param schemaRegistryName the logical name of the Schema Registry schemas are stored in.
     * @param topicName the name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the internal topic descriptor.
     */
    public static <K, V> KafkaTopicInternal<K, V> internalTopic(
            final String clusterName,
            final String schemaRegistryName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType) {
        return new InternalTopicDescriptor<>(
                clusterName, schemaRegistryName, topicName, keyType, valueType);
    }

    /**
     * Create a Kafka topic descriptor for a topic that is implicitly created.
     *
     * <p>Most internal topics, e.g. Kafka Streams changelog and repartition topics, are implicitly
     * created For such topics use {@link #internalTopic}
     *
     * <p>For an internal topic that you want Creek to create, use this method.
     *
     * @param topicName the name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param config the config of the topic.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the internal topic descriptor.
     */
    public static <K, V> CreatableKafkaTopicInternal<K, V> creatableInternalTopic(
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return creatableInternalTopic(
                DEFAULT_CLUSTER_NAME,
                DEFAULT_SCHEMA_REGISTRY_NAME,
                topicName,
                keyType,
                valueType,
                config);
    }

    /**
     * Create a Kafka topic descriptor for a topic that is implicitly created.
     *
     * <p>Most internal topics, e.g. Kafka Streams changelog and repartition topics, are implicitly
     * created For such topics use {@link #internalTopic}
     *
     * <p>For an internal topic that you want Creek to create, use this method.
     *
     * @param clusterName the logical name of the Kafka cluster the topic is in.
     * @param schemaRegistryName the logical name of the Schema Registry schemas are stored in.
     * @param topicName the name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param config the config of the topic.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the internal topic descriptor.
     */
    public static <K, V> CreatableKafkaTopicInternal<K, V> creatableInternalTopic(
            final String clusterName,
            final String schemaRegistryName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return new CreatableInternalTopicDescriptor<>(
                clusterName, schemaRegistryName, topicName, keyType, valueType, config);
    }

    /**
     * Create an output Kafka topic descriptor.
     *
     * <p>Looking for a version that returns {@link KafkaTopicOutput}? Get one of those by calling
     * {@link OwnedKafkaTopicInput#toOutput()} on the topic descriptor defined in the downstream
     * component.
     *
     * @param topicName the name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param config the config of the topic.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the output topic descriptor.
     */
    public static <K, V> OwnedKafkaTopicOutput<K, V> outputTopic(
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return outputTopic(
                DEFAULT_CLUSTER_NAME,
                DEFAULT_SCHEMA_REGISTRY_NAME,
                topicName,
                keyType,
                valueType,
                config);
    }

    /**
     * Create an output Kafka topic descriptor.
     *
     * <p>Looking for a version that returns {@link KafkaTopicOutput}? Get one of those by calling
     * {@link OwnedKafkaTopicInput#toOutput()} on the topic descriptor defined in the downstream
     * component.
     *
     * @param clusterName the logical name of the Kafka cluster the topic is in.
     * @param schemaRegistryName the logical name of the Schema Registry the schemas are in.
     * @param topicName the name of the topic
     * @param keyType the type serialized into the Kafka record key.
     * @param valueType the type serialized into the Kafka record value.
     * @param config the config of the topic.
     * @param <K> the type serialized into the Kafka record key.
     * @param <V> the type serialized into the Kafka record value.
     * @return the output topic descriptor.
     */
    public static <K, V> OwnedKafkaTopicOutput<K, V> outputTopic(
            final String clusterName,
            final String schemaRegistryName,
            final String topicName,
            final Class<K> keyType,
            final Class<V> valueType,
            final TopicConfigBuilder config) {
        return new OwnedOutputTopicDescriptor<>(
                clusterName, schemaRegistryName, topicName, keyType, valueType, config);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private abstract static class TopicDescriptor<K, V> implements KafkaTopicDescriptor<K, V> {
        private final String clusterName;
        private final String schemaRegistryName;
        private final String topicName;
        private final TopicPart<K> key;
        private final TopicPart<V> value;
        private final Optional<KafkaTopicConfig> config;

        TopicDescriptor(
                final String clusterName,
                final String schemaRegistryName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final Optional<TopicConfigBuilder> config) {
            this.clusterName = requireNonBlank(clusterName, "clusterName");
            this.schemaRegistryName = requireNonBlank(schemaRegistryName, "schemaRegistryName");
            this.topicName = requireNonBlank(topicName, "topicName");
            this.key = new TopicPart<>(Part.key, keyType, schemaRegistryName);
            this.value = new TopicPart<>(Part.value, valueType, schemaRegistryName);
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

        String schemaRegistryName() {
            return schemaRegistryName;
        }

        @Override
        public Stream<ResourceDescriptor> resources() {
            return Stream.of(key, value).flatMap(TopicPart::resources);
        }

        private final class TopicPart<T> implements PartDescriptor<T> {

            private final Part part;
            private final Class<T> type;
            private final Optional<? extends SchemaDescriptor<T>> schema;

            TopicPart(final Part part, final Class<T> type, final String schemaRegistryName) {
                this.part = requireNonNull(part, "part");
                this.type = requireNonNull(type, "type");
                this.schema =
                        NativeKafkaSerde.supports(type)
                                ? Optional.empty()
                                : Optional.of(
                                        topic() instanceof OwnedResource
                                                ? new OwnedJsonSchema(schemaRegistryName)
                                                : new UnownedJsonSchema(schemaRegistryName));
            }

            @Override
            public Part name() {
                return part;
            }

            @Override
            public SerializationFormat format() {
                return schema.isPresent()
                        ? JsonSchemaKafkaSerde.format()
                        : NativeKafkaSerde.format();
            }

            @Override
            public Class<T> type() {
                return type;
            }

            @Override
            public KafkaTopicDescriptor<?, ?> topic() {
                return TopicDescriptor.this;
            }

            @Override
            public Stream<? extends ResourceDescriptor> resources() {
                return schema.stream();
            }

            public Optional<? extends SchemaDescriptor<T>> schema() {
                return schema;
            }

            private abstract class BaseJsonSchema implements JsonSchemaDescriptor<T> {

                final String schemaRegistryName;

                private BaseJsonSchema(final String schemaRegistryName) {
                    this.schemaRegistryName =
                            requireNonBlank(schemaRegistryName, "schemaRegistryName");
                }

                @Override
                public String schemaRegistryName() {
                    return schemaRegistryName;
                }

                @Override
                public KafkaTopicDescriptor.PartDescriptor<T> part() {
                    return TopicPart.this;
                }
            }

            private final class OwnedJsonSchema extends BaseJsonSchema
                    implements OwnedJsonSchemaDescriptor<T> {
                private OwnedJsonSchema(final String schemaRegistryName) {
                    super(schemaRegistryName);
                }
            }

            private final class UnownedJsonSchema extends BaseJsonSchema
                    implements UnownedJsonSchemaDescriptor<T> {

                private UnownedJsonSchema(final String schemaRegistryName) {
                    super(schemaRegistryName);
                }
            }
        }
    }

    private static final class OwnedOutputTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements OwnedKafkaTopicOutput<K, V> {

        OwnedOutputTopicDescriptor(
                final String clusterName,
                final String schemaRegistryName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final TopicConfigBuilder config) {
            super(
                    clusterName,
                    schemaRegistryName,
                    topicName,
                    keyType,
                    valueType,
                    Optional.of(config));
        }

        @Override
        public KafkaTopicInput<K, V> toInput() {
            return new UnownedInputTopicDescriptor<>(
                    cluster(), schemaRegistryName(), name(), key().type(), value().type());
        }
    }

    private static final class OwnedInputTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements OwnedKafkaTopicInput<K, V> {

        OwnedInputTopicDescriptor(
                final String clusterName,
                final String schemaRegistryName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final TopicConfigBuilder config) {
            super(
                    clusterName,
                    schemaRegistryName,
                    topicName,
                    keyType,
                    valueType,
                    Optional.of(config));
        }

        @Override
        public KafkaTopicOutput<K, V> toOutput() {
            return new UnownedOutputTopicDescriptor<>(
                    cluster(), schemaRegistryName(), name(), key().type(), value().type());
        }
    }

    private static final class UnownedOutputTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements KafkaTopicOutput<K, V> {

        UnownedOutputTopicDescriptor(
                final String clusterName,
                final String schemaRegistryName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType) {
            super(clusterName, schemaRegistryName, topicName, keyType, valueType, Optional.empty());
        }
    }

    private static final class UnownedInputTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements KafkaTopicInput<K, V> {

        UnownedInputTopicDescriptor(
                final String clusterName,
                final String schemaRegistryName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType) {
            super(clusterName, schemaRegistryName, topicName, keyType, valueType, Optional.empty());
        }
    }

    private static final class InternalTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements KafkaTopicInternal<K, V> {

        InternalTopicDescriptor(
                final String clusterName,
                final String schemaRegistryName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType) {
            super(clusterName, schemaRegistryName, topicName, keyType, valueType, Optional.empty());
        }
    }

    private static final class CreatableInternalTopicDescriptor<K, V> extends TopicDescriptor<K, V>
            implements CreatableKafkaTopicInternal<K, V> {

        CreatableInternalTopicDescriptor(
                final String clusterName,
                final String schemaRegistryName,
                final String topicName,
                final Class<K> keyType,
                final Class<V> valueType,
                final TopicConfigBuilder config) {
            super(
                    clusterName,
                    schemaRegistryName,
                    topicName,
                    keyType,
                    valueType,
                    Optional.of(config));
        }
    }

    public static final class TopicConfigBuilder {

        private static final int MAX_PARTITIONS = 10_000;

        private final int partitions;
        private final Map<String, String> config = new HashMap<>();

        public static TopicConfigBuilder withPartitions(final int partitions) {
            return builder(partitions, false);
        }

        public static TopicConfigBuilder builder(final int partitions, final boolean allowCrazy) {
            if (partitions <= 0) {
                final NumberFormat format = NumberFormat.getInstance(Locale.ROOT);
                throw new IllegalArgumentException(
                        "partition count must be positive, but was " + format.format(partitions));
            }
            if (!allowCrazy && partitions > MAX_PARTITIONS) {
                final NumberFormat format = NumberFormat.getInstance(Locale.ROOT);
                throw new IllegalArgumentException(
                        "partition count should be less than "
                                + format.format(MAX_PARTITIONS)
                                + ", but was "
                                + format.format(partitions));
            }
            return new TopicConfigBuilder(partitions);
        }

        private TopicConfigBuilder(final int partitions) {
            this.partitions = partitions;
        }

        public TopicConfigBuilder withConfig(final String key, final String value) {
            this.config.put(requireNonNull(key, "key"), requireNonNull(value, "value"));
            return this;
        }

        public TopicConfigBuilder withConfigs(final Map<String, String> config) {
            config.forEach(this::withConfig);
            return this;
        }

        public TopicConfigBuilder withKeyCompaction() {
            return withConfig("cleanup.policy", "compact");
        }

        public TopicConfigBuilder withKeyCompactionAndDeletion() {
            return withConfig("cleanup.policy", "compact,delete");
        }

        public TopicConfigBuilder withRetentionTime(final Duration duration) {
            return withConfig("retention.ms", String.valueOf(duration.toMillis()));
        }

        public TopicConfigBuilder withInfiniteRetention() {
            return withConfig("retention.ms", "-1");
        }

        public TopicConfigBuilder withSegmentSize(final long segmentBytes) {
            return withConfig("segment.bytes", String.valueOf(segmentBytes));
        }

        public KafkaTopicConfig build() {
            return new TopicConfig(partitions, config);
        }

        private static final class TopicConfig implements KafkaTopicConfig {

            private final int partitions;
            private final Map<String, String> config;

            TopicConfig(final int partitions, final Map<String, String> config) {
                this.partitions = partitions;
                this.config = Map.copyOf(requireNonNull(config, "config"));
            }

            @Override
            public int partitions() {
                return partitions;
            }

            @Override
            public Map<String, String> config() {
                return config;
            }
        }
    }
}
