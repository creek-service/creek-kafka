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

package org.creekservice.internal.kafka.extension.resource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.Map;
import org.creekservice.api.kafka.metadata.KafkaTopicConfig;
import org.junit.jupiter.api.Test;

class KafkaTopicConfigsTest {

    private final KafkaTopicConfig config = new FirstTopicConfig(1);

    @Test
    void shouldMatchIfAttributesMatch() {
        assertThat(
                KafkaTopicConfigs.matches(
                        config, new SecondTopicConfig(config.partitions(), config.config())),
                is(true));
    }

    @Test
    void shouldNotMatchIfPartitionsDiffer() {
        assertThat(
                KafkaTopicConfigs.matches(
                        config, new SecondTopicConfig(config.partitions() + 1, config.config())),
                is(false));
    }

    @Test
    void shouldNotMatchIfConfigDiffer() {
        assertThat(
                KafkaTopicConfigs.matches(
                        config, new SecondTopicConfig(config.partitions(), Map.of("diff", "diff"))),
                is(false));
    }

    @Test
    void shouldImplementAsString() {
        assertThat(
                KafkaTopicConfigs.asString(new SecondTopicConfig(17, Map.of("a", "b"))),
                is("SecondTopicConfig[" + "partitions=17, " + "config={a=b}" + "]"));
    }

    @Test
    void shouldImplementHashCode() {
        // Given:
        final int hashCode =
                KafkaTopicConfigs.hashCode(new SecondTopicConfig(17, Map.of("a", "b")));

        // Then:
        assertThat(
                KafkaTopicConfigs.hashCode(new SecondTopicConfig(17, Map.of("a", "b"))),
                is(hashCode));
        assertThat(
                KafkaTopicConfigs.hashCode(new SecondTopicConfig(0, Map.of("a", "b"))),
                is(not(hashCode)));
        assertThat(
                KafkaTopicConfigs.hashCode(new SecondTopicConfig(17, Map.of())), is(not(hashCode)));
    }

    private static final class FirstTopicConfig implements KafkaTopicConfig {

        private final int partitions;

        FirstTopicConfig(final int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int partitions() {
            return partitions;
        }
    }

    private static final class SecondTopicConfig implements KafkaTopicConfig {

        private final int partitions;
        private final Map<String, String> config;

        SecondTopicConfig(final int partitions, final Map<String, String> config) {
            this.partitions = partitions;
            this.config = config;
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
