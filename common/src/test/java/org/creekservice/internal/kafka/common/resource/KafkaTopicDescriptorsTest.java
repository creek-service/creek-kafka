/*
 * Copyright 2021-2022 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.common.resource;

import static org.creekservice.api.kafka.metadata.KafkaResourceIds.topicId;
import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicConfig;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.junit.jupiter.api.Test;

class KafkaTopicDescriptorsTest {

    private static final SerializationFormat FORMAT_A = serializationFormat("A");
    private static final SerializationFormat FORMAT_B = serializationFormat("B");
    private static final URI RES_ID = topicId("c", "t");

    private final KafkaTopicConfig config = new TestConfig(1);
    private final KafkaTopicDescriptor<?, ?> unowned =
            new FirstKafkaTopic<>("bob", "c1", FORMAT_A, FORMAT_B, long.class, String.class);
    private final CreatableKafkaTopic<?, ?> owned =
            new FirstCreatableKafkaTopic<>(
                    "peter", "c1", FORMAT_B, FORMAT_A, String.class, long.class, config);

    @Test
    void shouldMatchUnownedIfAttributesMatch() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                unowned.id(),
                                unowned.name(),
                                unowned.cluster(),
                                unowned.key().format(),
                                unowned.value().format(),
                                unowned.key().type(),
                                unowned.value().type())),
                is(true));
    }

    @Test
    void shouldMatchOwnedIfAttributesMatch() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.id(),
                                owned.name(),
                                owned.cluster(),
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                owned.config())),
                is(true));
    }

    @Test
    void shouldNotMatchUnownedIfIdDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                URI.create("diff:/res"),
                                unowned.name(),
                                unowned.cluster(),
                                unowned.key().format(),
                                unowned.value().format(),
                                unowned.key().type(),
                                unowned.value().type())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfNameDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                RES_ID,
                                "diff",
                                unowned.cluster(),
                                unowned.key().format(),
                                unowned.value().format(),
                                unowned.key().type(),
                                unowned.value().type())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfClusterDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                RES_ID,
                                unowned.name(),
                                "diff",
                                unowned.key().format(),
                                unowned.value().format(),
                                unowned.key().type(),
                                unowned.value().type())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfKeyFormatDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                RES_ID,
                                owned.name(),
                                owned.cluster(),
                                FORMAT_B,
                                unowned.value().format(),
                                unowned.key().type(),
                                unowned.value().type())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfValueFormatDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                RES_ID,
                                unowned.name(),
                                unowned.cluster(),
                                unowned.key().format(),
                                FORMAT_A,
                                unowned.key().type(),
                                unowned.value().type())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfKeyTypeDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                RES_ID,
                                unowned.name(),
                                unowned.cluster(),
                                unowned.key().format(),
                                unowned.value().format(),
                                Void.class,
                                unowned.value().type())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfValueTypeDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                RES_ID,
                                unowned.name(),
                                unowned.cluster(),
                                unowned.key().format(),
                                unowned.value().format(),
                                unowned.key().type(),
                                Void.class)),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIIdDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                URI.create("diff:/res"),
                                owned.name(),
                                owned.cluster(),
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfNameDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                "Diff",
                                owned.cluster(),
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfClusterDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.name(),
                                "Diff",
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfKeyFormatDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.name(),
                                owned.cluster(),
                                serializationFormat("diff"),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfValueFormatDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.name(),
                                owned.cluster(),
                                owned.key().format(),
                                serializationFormat("diff"),
                                owned.key().type(),
                                owned.value().type(),
                                owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfKeyTypeDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.name(),
                                owned.cluster(),
                                owned.key().format(),
                                owned.value().format(),
                                Void.class,
                                owned.value().type(),
                                owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfValueTypeDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.name(),
                                owned.cluster(),
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                Void.class,
                                owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfConfigDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.name(),
                                owned.cluster(),
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                new TestConfig(owned.config().partitions() + 1))),
                is(false));
    }

    @Test
    void shouldMatchIfAttributesMatchButOnlyOneOwned() {
        // Given:
        final SecondKafkaTopic<?, ?> unowned =
                new SecondKafkaTopic<>(
                        owned.id(),
                        owned.name(),
                        owned.cluster(),
                        owned.key().format(),
                        owned.value().format(),
                        owned.key().type(),
                        owned.value().type());

        // Then:
        assertThat(KafkaTopicDescriptors.matches(owned, unowned), is(true));
        assertThat(KafkaTopicDescriptors.matches(unowned, owned), is(true));
    }

    @Test
    void shouldImplementUnownedAsString() {
        assertThat(
                KafkaTopicDescriptors.asString(unowned),
                is(
                        "FirstKafkaTopic["
                                + "id=kafka-topic://c/t, "
                                + "name=bob, "
                                + "cluster=c1, "
                                + "key=TestPart[format=A, type=long], "
                                + "value=TestPart[format=B, type=java.lang.String]"
                                + "]"));
    }

    @Test
    void shouldImplementOwnedAsString() {
        assertThat(
                KafkaTopicDescriptors.asString(owned),
                is(
                        "FirstCreatableKafkaTopic["
                                + "id=kafka-topic://c1/peter, "
                                + "name=peter, "
                                + "cluster=c1, "
                                + "key=TestPart[format=B, type=java.lang.String], "
                                + "value=TestPart[format=A, type=long], "
                                + "config="
                                + KafkaTopicConfigs.asString(config)
                                + "]"));
    }

    @Test
    void shouldDefaultToDefaultCluster() {
        // Given:
        final KafkaTopicDescriptor<?, ?> defaultDescriptor =
                new KafkaTopicDescriptor<>() {
                    @Override
                    public URI id() {
                        return null;
                    }

                    @Override
                    public String name() {
                        return null;
                    }

                    @Override
                    public PartDescriptor<Object> key() {
                        return null;
                    }

                    @Override
                    public PartDescriptor<Object> value() {
                        return null;
                    }
                };

        // Then:
        assertThat(defaultDescriptor.cluster(), is(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME));
    }

    @Test
    void shouldNotBlowUpOnNulls() {
        // Given:
        final KafkaTopicDescriptor<?, ?> descriptor = mock(KafkaTopicDescriptor.class);

        // Then:
        assertThat(
                KafkaTopicDescriptors.asString(descriptor),
                containsString("[id=null, name=null, cluster=null, key=null, value=null]"));
    }

    @Test
    void shouldNotBlowUpOnNullConfig() {
        // Given:
        final KafkaTopicDescriptor<?, ?> descriptor = mock(OwnedKafkaTopicOutput.class);

        // Then:
        assertThat(
                KafkaTopicDescriptors.asString(descriptor),
                containsString(
                        "[id=null, name=null, cluster=null, key=null, value=null, config=null]"));
    }

    @Test
    void shouldNotBlowUpOnPartNulls() {
        // Given:
        final KafkaTopicDescriptor.PartDescriptor<?> descriptor =
                mock(KafkaTopicDescriptor.PartDescriptor.class);

        // Then:
        assertThat(
                KafkaTopicDescriptors.asString(descriptor),
                containsString("[format=null, type=null]"));
    }

    @Test
    void shouldCalculateHashCode() {
        // Given:
        final int hashCode = KafkaTopicDescriptors.hashCode(owned);

        // Then:
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new SecondCreatableKafkaTopic<>(
                                "peter",
                                "c1",
                                FORMAT_B,
                                FORMAT_A,
                                String.class,
                                long.class,
                                config)),
                is(hashCode));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstKafkaTopic<>(
                                "peter", "c1", FORMAT_B, FORMAT_A, String.class, long.class)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstCreatableKafkaTopic<>(
                                "diff",
                                "c1",
                                FORMAT_B,
                                FORMAT_A,
                                String.class,
                                long.class,
                                config)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstCreatableKafkaTopic<>(
                                "peter",
                                "diff",
                                FORMAT_B,
                                FORMAT_A,
                                String.class,
                                long.class,
                                config)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstCreatableKafkaTopic<>(
                                "peter",
                                "c1",
                                FORMAT_A,
                                FORMAT_A,
                                String.class,
                                long.class,
                                config)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstCreatableKafkaTopic<>(
                                "peter",
                                "c1",
                                FORMAT_B,
                                FORMAT_B,
                                String.class,
                                long.class,
                                config)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstCreatableKafkaTopic<>(
                                "peter", "c1", FORMAT_B, FORMAT_A, Void.class, long.class, config)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstCreatableKafkaTopic<>(
                                "peter",
                                "c1",
                                FORMAT_B,
                                FORMAT_A,
                                String.class,
                                Void.class,
                                config)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCode(
                        new FirstCreatableKafkaTopic<>(
                                "peter",
                                "c1",
                                FORMAT_B,
                                FORMAT_A,
                                String.class,
                                long.class,
                                mock(KafkaTopicConfig.class))),
                is(not(hashCode)));
    }

    @Test
    void shouldCalculateHashCodeIgnoringConfig() {
        // Given:
        final int hashCode =
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "t", "c", FORMAT_A, FORMAT_B, long.class, String.class));

        // Then:
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "t", "c", FORMAT_A, FORMAT_B, long.class, String.class)),
                is(hashCode));
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstCreatableKafkaTopic<>(
                                "t", "c", FORMAT_A, FORMAT_B, long.class, String.class, config)),
                is(hashCode));
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "diff", "c1", FORMAT_A, FORMAT_B, long.class, String.class)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "bob", "diff", FORMAT_A, FORMAT_B, long.class, String.class)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "t", "c", FORMAT_B, FORMAT_B, long.class, String.class)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "t", "c", FORMAT_A, FORMAT_A, long.class, String.class)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "t", "c", FORMAT_A, FORMAT_B, String.class, String.class)),
                is(not(hashCode)));
        assertThat(
                KafkaTopicDescriptors.hashCodeIgnoringConfig(
                        new FirstKafkaTopic<>(
                                "t", "c", FORMAT_A, FORMAT_B, long.class, long.class)),
                is(not(hashCode)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldCalculateHashCodeOfPart() {
        // Given:
        final int hashCode = KafkaTopicDescriptors.hashCode(unowned.key());
        final KafkaTopicDescriptor.PartDescriptor<?> part =
                mock(KafkaTopicDescriptor.PartDescriptor.class);
        when(part.type()).thenReturn((Class) unowned.key().type());
        when(part.format()).thenReturn(unowned.key().format());

        // Then:
        assertThat(KafkaTopicDescriptors.hashCode(part), is(hashCode));
        when(part.type()).thenReturn((Class) Void.class);
        assertThat(KafkaTopicDescriptors.hashCode(part), is(not(hashCode)));
        when(part.type()).thenReturn((Class) unowned.key().type());
        when(part.format()).thenReturn(serializationFormat("diff"));
        assertThat(KafkaTopicDescriptors.hashCode(part), is(not(hashCode)));
    }

    private static final class FirstKafkaTopic<K, V> implements KafkaTopicDescriptor<K, V> {

        private final URI id;
        private final String name;
        private final String cluster;
        private final TestPart<K> key;
        private final TestPart<V> value;

        FirstKafkaTopic(
                final String name,
                final String cluster,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType) {
            this.id = RES_ID;
            this.name = name;
            this.cluster = cluster;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String cluster() {
            return cluster;
        }

        @Override
        public PartDescriptor<K> key() {
            return key;
        }

        @Override
        public PartDescriptor<V> value() {
            return value;
        }

        private static final class TestPart<T> implements PartDescriptor<T> {
            private final Class<T> type;
            private final SerializationFormat format;

            TestPart(final Class<T> type, final SerializationFormat format) {
                this.type = type;
                this.format = format;
            }

            @Override
            public SerializationFormat format() {
                return format;
            }

            @Override
            public Class<T> type() {
                return type;
            }
        }
    }

    private static final class SecondKafkaTopic<K, V> implements KafkaTopicDescriptor<K, V> {

        private final URI id;
        private final String name;
        private final String cluster;
        private final TestPart<K> key;
        private final TestPart<V> value;

        SecondKafkaTopic(
                final URI id,
                final String name,
                final String cluster,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType) {
            this.id = id;
            this.name = name;
            this.cluster = cluster;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String cluster() {
            return cluster;
        }

        @Override
        public PartDescriptor<K> key() {
            return key;
        }

        @Override
        public PartDescriptor<V> value() {
            return value;
        }

        private static final class TestPart<T> implements PartDescriptor<T> {
            private final Class<T> type;
            private final SerializationFormat format;

            TestPart(final Class<T> type, final SerializationFormat format) {
                this.type = type;
                this.format = format;
            }

            @Override
            public SerializationFormat format() {
                return format;
            }

            @Override
            public Class<T> type() {
                return type;
            }
        }
    }

    private static final class FirstCreatableKafkaTopic<K, V> implements CreatableKafkaTopic<K, V> {

        private final URI id;
        private final String name;
        private final String cluster;
        private final TestPart<K> key;
        private final TestPart<V> value;
        private final KafkaTopicConfig config;

        FirstCreatableKafkaTopic(
                final String name,
                final String cluster,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType,
                final KafkaTopicConfig config) {
            this.id = topicId(cluster, name);
            this.name = name;
            this.cluster = cluster;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
            this.config = config;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String cluster() {
            return cluster;
        }

        @Override
        public PartDescriptor<K> key() {
            return key;
        }

        @Override
        public PartDescriptor<V> value() {
            return value;
        }

        @Override
        public KafkaTopicConfig config() {
            return config;
        }

        private static final class TestPart<T> implements PartDescriptor<T> {
            private final Class<T> type;
            private final SerializationFormat format;

            TestPart(final Class<T> type, final SerializationFormat format) {
                this.type = type;
                this.format = format;
            }

            @Override
            public SerializationFormat format() {
                return format;
            }

            @Override
            public Class<T> type() {
                return type;
            }
        }
    }

    private static final class SecondCreatableKafkaTopic<K, V>
            implements CreatableKafkaTopic<K, V> {

        private final URI id;
        private final String name;
        private final String cluster;
        private final TestPart<K> key;
        private final TestPart<V> value;
        private final KafkaTopicConfig config;

        SecondCreatableKafkaTopic(
                final String name,
                final String cluster,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType,
                final KafkaTopicConfig config) {
            this(
                    topicId(cluster, name),
                    name,
                    cluster,
                    keyFormat,
                    valueFormat,
                    keyType,
                    valueType,
                    config);
        }

        @SuppressWarnings("checkstyle:ParameterNumber")
        SecondCreatableKafkaTopic(
                final URI id,
                final String name,
                final String cluster,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType,
                final KafkaTopicConfig config) {
            this.id = id;
            this.name = name;
            this.cluster = cluster;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
            this.config = config;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String cluster() {
            return cluster;
        }

        @Override
        public PartDescriptor<K> key() {
            return key;
        }

        @Override
        public PartDescriptor<V> value() {
            return value;
        }

        @Override
        public KafkaTopicConfig config() {
            return config;
        }

        private static final class TestPart<T> implements PartDescriptor<T> {
            private final Class<T> type;
            private final SerializationFormat format;

            TestPart(final Class<T> type, final SerializationFormat format) {
                this.type = type;
                this.format = format;
            }

            @Override
            public SerializationFormat format() {
                return format;
            }

            @Override
            public Class<T> type() {
                return type;
            }
        }
    }

    private static final class TestConfig implements KafkaTopicConfig {

        private final int partitions;

        TestConfig(final int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int partitions() {
            return partitions;
        }
    }
}
