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

package org.creek.api.kafka.metadata;

import static org.creek.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class KafkaTopicDescriptorsTest {

    private static final SerializationFormat FORMAT_A = serializationFormat("A");
    private static final SerializationFormat FORMAT_B = serializationFormat("B");

    private final KafkaTopicConfig config = new TestConfig(1);
    private final KafkaTopicDescriptor<?, ?> unowned =
            new FirstKafkaTopic<>("bob", FORMAT_A, FORMAT_B, long.class, String.class);
    private final CreatableKafkaTopic<?, ?> owned =
            new FirstCreatableKafkaTopic<>(
                    "peter", FORMAT_B, FORMAT_A, String.class, long.class, config);

    @Test
    void shouldMatchUnownedIfAttributesMatch() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                unowned.name(),
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
                                owned.name(),
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                owned.config())),
                is(true));
    }

    @Test
    void shouldNotMatchUnownedIfNameDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        unowned,
                        new SecondKafkaTopic<>(
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
                                owned.name(),
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
                                owned.name(),
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
                                owned.name(),
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
                                owned.name(),
                                unowned.key().format(),
                                unowned.value().format(),
                                unowned.key().type(),
                                Void.class)),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfNameDifferent() {
        assertThat(
                KafkaTopicDescriptors.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
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
                                owned.key().format(),
                                owned.value().format(),
                                owned.key().type(),
                                owned.value().type(),
                                new TestConfig(owned.config().getPartitions() + 1))),
                is(false));
    }

    @Test
    void shouldMatchIfAttributesMatchButOnlyOneOwned() {
        // Given:
        final SecondKafkaTopic<?, ?> unowned =
                new SecondKafkaTopic<>(
                        owned.name(),
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
                                + "name=bob, "
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
                                + "name=peter, "
                                + "key=TestPart[format=B, type=java.lang.String], "
                                + "value=TestPart[format=A, type=long], "
                                + "config="
                                + KafkaTopicConfig.asString(config)
                                + "]"));
    }

    private static final class FirstKafkaTopic<K, V> implements KafkaTopicDescriptor<K, V> {

        private final String name;
        private final TestPart<K> key;
        private final TestPart<V> value;

        FirstKafkaTopic(
                final String name,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType) {
            this.name = name;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
        }

        @Override
        public String name() {
            return name;
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

        private final String name;
        private final TestPart<K> key;
        private final TestPart<V> value;

        SecondKafkaTopic(
                final String name,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType) {
            this.name = name;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
        }

        @Override
        public String name() {
            return name;
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

        private final String name;
        private final TestPart<K> key;
        private final TestPart<V> value;
        private final KafkaTopicConfig config;

        FirstCreatableKafkaTopic(
                final String name,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType,
                final KafkaTopicConfig config) {
            this.name = name;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
            this.config = config;
        }

        @Override
        public String name() {
            return name;
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

        private final String name;
        private final TestPart<K> key;
        private final TestPart<V> value;
        private final KafkaTopicConfig config;

        SecondCreatableKafkaTopic(
                final String name,
                final SerializationFormat keyFormat,
                final SerializationFormat valueFormat,
                final Class<K> keyType,
                final Class<V> valueType,
                final KafkaTopicConfig config) {
            this.name = name;
            this.key = new TestPart<>(keyType, keyFormat);
            this.value = new TestPart<>(valueType, valueFormat);
            this.config = config;
        }

        @Override
        public String name() {
            return name;
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
        public int getPartitions() {
            return partitions;
        }
    }
}
