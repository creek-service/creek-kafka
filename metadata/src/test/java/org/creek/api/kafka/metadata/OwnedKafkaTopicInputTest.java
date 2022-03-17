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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class OwnedKafkaTopicInputTest {

    private final OwnedKafkaTopicInput<Long, String> input = new TestInput();

    @Test
    void shouldConvertToOutput() {
        // When:
        final KafkaTopicOutput<Long, String> output = input.toOutput();

        // Then:
        assertThat(output.name(), is(input.name()));
        assertThat(output.keyType(), is(input.keyType()));
        assertThat(output.valueType(), is(input.valueType()));
    }

    private static final class TestInput implements OwnedKafkaTopicInput<Long, String> {

        @Override
        public KafkaTopicConfig config() {
            return null;
        }

        @Override
        public String name() {
            return "bob";
        }

        @Override
        public Class<Long> keyType() {
            return Long.class;
        }

        @Override
        public Class<String> valueType() {
            return String.class;
        }
    }
}
