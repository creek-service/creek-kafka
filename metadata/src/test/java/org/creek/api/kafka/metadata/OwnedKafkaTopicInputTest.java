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
import static org.hamcrest.Matchers.sameInstance;

import org.junit.jupiter.api.Test;

class OwnedKafkaTopicInputTest {

    private final OwnedKafkaTopicInput<Long, String> input = new TestInput();

    @Test
    void shouldConvertToOutput() {
        // When:
        final KafkaTopicOutput<Long, String> output = input.toOutput();

        // Then:
        assertThat(output.name(), is(input.name()));
        assertThat(output.key(), is(sameInstance(input.key())));
        assertThat(output.value(), is(sameInstance(input.value())));
    }

    private static final class TestInput implements OwnedKafkaTopicInput<Long, String> {

        private final TestPart<Long> key =
                new TestPart<>(Long.class, serializationFormat("keyFormat"));
        private final TestPart<String> value =
                new TestPart<>(String.class, serializationFormat("valueFormat"));

        @Override
        public KafkaTopicConfig config() {
            return null;
        }

        @Override
        public String name() {
            return "bob";
        }

        @Override
        public PartDescriptor<Long> key() {
            return key;
        }

        @Override
        public PartDescriptor<String> value() {
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
}
