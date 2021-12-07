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

import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaTopicConfigTest {

    private final KafkaTopicConfig config = new FirstTopicConfig(1);

    @Test
    void shouldMatchIfAttributesMatch() {
        assertThat(
                KafkaTopicConfig.matches(
                        config, new SecondTopicConfig(config.getPartitions(), config.getConfig())),
                is(true));
    }

    @Test
    void shouldNotMatchIfPartitionsDiffer() {
        assertThat(
                KafkaTopicConfig.matches(
                        config,
                        new SecondTopicConfig(config.getPartitions() + 1, config.getConfig())),
                is(false));
    }

    @Test
    void shouldNotMatchIfConfigDiffer() {
        assertThat(
                KafkaTopicConfig.matches(
                        config,
                        new SecondTopicConfig(config.getPartitions(), Map.of("diff", "diff"))),
                is(false));
    }

    @Test
    void shouldImplementAsString() {
        assertThat(
                KafkaTopicConfig.asString(new SecondTopicConfig(17, Map.of("a", "b"))),
                is("SecondTopicConfig[" + "partitions=17, " + "config={a=b}" + "]"));
    }

    private static final class FirstTopicConfig implements KafkaTopicConfig {

        private final int partitions;

        FirstTopicConfig(final int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int getPartitions() {
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
        public int getPartitions() {
            return partitions;
        }

        @Override
        public Map<String, String> getConfig() {
            return config;
        }
    }
}
