/*
 * Copyright 2021 Creek Contributors (https://github.com/creek-service)
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class KafkaTopicTest {

    private final KafkaTopicConfig config = new TestConfig(1);
    private final KafkaTopic<?, ?> unowned = new FirstKafkaTopic<>("bob", long.class, String.class);
    private final CreatableKafkaTopic<?, ?> owned =
            new FirstCreatableKafkaTopic<>("bob", long.class, String.class, config);

    @Test
    void shouldMatchUnownedIfAttributesMatch() {
        assertThat(
                KafkaTopic.matches(
                        unowned,
                        new SecondKafkaTopic<>(
                                unowned.topicName(), unowned.keyType(), unowned.valueType())),
                is(true));
    }

    @Test
    void shouldMatchOwnedIfAttributesMatch() {
        assertThat(
                KafkaTopic.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.topicName(),
                                owned.keyType(),
                                owned.valueType(),
                                owned.config())),
                is(true));
    }

    @Test
    void shouldNotMatchUnownedIfNameDifferent() {
        assertThat(
                KafkaTopic.matches(
                        unowned,
                        new SecondKafkaTopic<>("diff", unowned.keyType(), unowned.valueType())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfKeyTypeDifferent() {
        assertThat(
                KafkaTopic.matches(
                        unowned,
                        new SecondKafkaTopic<>(owned.topicName(), Void.class, unowned.valueType())),
                is(false));
    }

    @Test
    void shouldNotMatchUnownedIfValueTypeDifferent() {
        assertThat(
                KafkaTopic.matches(
                        unowned,
                        new SecondKafkaTopic<>(owned.topicName(), unowned.keyType(), Void.class)),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfNameDifferent() {
        assertThat(
                KafkaTopic.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                "Diff", owned.keyType(), owned.valueType(), owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfKeyTypeDifferent() {
        assertThat(
                KafkaTopic.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.topicName(), Void.class, owned.valueType(), owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfValueTypeDifferent() {
        assertThat(
                KafkaTopic.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.topicName(), owned.keyType(), Void.class, owned.config())),
                is(false));
    }

    @Test
    void shouldNotMatchOwnedIfConfigDifferent() {
        assertThat(
                KafkaTopic.matches(
                        owned,
                        new SecondCreatableKafkaTopic<>(
                                owned.topicName(),
                                owned.keyType(),
                                owned.valueType(),
                                new TestConfig(owned.config().partitions() + 1))),
                is(false));
    }

    @Test
    void shouldNotMatchIfOnlyOneOwned() {
        assertThat(KafkaTopic.matches(owned, unowned), is(false));
        assertThat(KafkaTopic.matches(unowned, owned), is(false));
    }

    @Test
    void shouldImplementUnownedAsString() {
        assertThat(
                KafkaTopic.asString(unowned),
                is(
                        "FirstKafkaTopic["
                                + "topicName=bob, "
                                + "keyType=long, "
                                + "valueType=java.lang.String"
                                + "]"));
    }

    @Test
    void shouldImplementOwnedAsString() {
        assertThat(
                KafkaTopic.asString(owned),
                is(
                        "FirstCreatableKafkaTopic["
                                + "topicName=bob, "
                                + "keyType=long, "
                                + "valueType=java.lang.String, "
                                + "config="
                                + KafkaTopicConfig.asString(config)
                                + "]"));
    }

    private static final class FirstKafkaTopic<K, V> implements KafkaTopic<K, V> {

        private final String name;
        private final Class<K> keyType;
        private final Class<V> valueType;

        FirstKafkaTopic(final String name, final Class<K> keyType, final Class<V> valueType) {
            this.name = name;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public String topicName() {
            return name;
        }

        @Override
        public Class<K> keyType() {
            return keyType;
        }

        @Override
        public Class<V> valueType() {
            return valueType;
        }
    }

    private static final class SecondKafkaTopic<K, V> implements KafkaTopic<K, V> {

        private final String name;
        private final Class<K> keyType;
        private final Class<V> valueType;

        SecondKafkaTopic(final String name, final Class<K> keyType, final Class<V> valueType) {
            this.name = name;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public String topicName() {
            return name;
        }

        @Override
        public Class<K> keyType() {
            return keyType;
        }

        @Override
        public Class<V> valueType() {
            return valueType;
        }
    }

    private static final class FirstCreatableKafkaTopic<K, V> implements CreatableKafkaTopic<K, V> {

        private final String name;
        private final Class<K> keyType;
        private final Class<V> valueType;
        private final KafkaTopicConfig config;

        FirstCreatableKafkaTopic(
                final String name,
                final Class<K> keyType,
                final Class<V> valueType,
                final KafkaTopicConfig config) {
            this.name = name;
            this.keyType = keyType;
            this.valueType = valueType;
            this.config = config;
        }

        @Override
        public String topicName() {
            return name;
        }

        @Override
        public Class<K> keyType() {
            return keyType;
        }

        @Override
        public Class<V> valueType() {
            return valueType;
        }

        @Override
        public KafkaTopicConfig config() {
            return config;
        }
    }

    private static final class SecondCreatableKafkaTopic<K, V>
            implements CreatableKafkaTopic<K, V> {

        private final String name;
        private final Class<K> keyType;
        private final Class<V> valueType;
        private final KafkaTopicConfig config;

        SecondCreatableKafkaTopic(
                final String name,
                final Class<K> keyType,
                final Class<V> valueType,
                final KafkaTopicConfig config) {
            this.name = name;
            this.keyType = keyType;
            this.valueType = valueType;
            this.config = config;
        }

        @Override
        public String topicName() {
            return name;
        }

        @Override
        public Class<K> keyType() {
            return keyType;
        }

        @Override
        public Class<V> valueType() {
            return valueType;
        }

        @Override
        public KafkaTopicConfig config() {
            return config;
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
