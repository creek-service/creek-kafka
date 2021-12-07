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

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class OwnedKafkaTopicOutputTest {

    private final OwnedKafkaTopicOutput<Long, String> output = new TestOutput();

    @Test
    void shouldConvertToInput() {
        // When:
        final KafkaTopicInput<Long, String> input = output.toInput();

        // Then:
        assertThat(input.getTopicName(), is(output.getTopicName()));
        assertThat(input.getKeyType(), is(output.getKeyType()));
        assertThat(input.getValueType(), is(output.getValueType()));
    }

    private static final class TestOutput implements OwnedKafkaTopicOutput<Long, String> {

        @Override
        public KafkaTopicConfig getConfig() {
            return null;
        }

        @Override
        public String getTopicName() {
            return "bob";
        }

        @Override
        public Class<Long> getKeyType() {
            return Long.class;
        }

        @Override
        public Class<String> getValueType() {
            return String.class;
        }
    }
}