/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.test.api.kafka.serde.eight.test;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.creekservice.api.kafka.serde.test.KafkaSerdeProviderTester;
import org.junit.jupiter.api.Test;

class KafkaSerdeProviderTesterTest {
    @Test
    void shouldPass() {
        // begin-snippet: serde-tester
        KafkaSerdeProviderTester.tester(ExampleTestSerdeProvider.class)
                .withExpectedFormat(serializationFormat("example"))
                .test();
        // end-snippet
    }

    @Test
    void shouldFailIfNotRegisteredInMetaInfServices() {
        // Given:
        final KafkaSerdeProviderTester tester =
                KafkaSerdeProviderTester.tester(BadJava8TestSerdeProvider.class);

        // When:
        final Error e = assertThrows(AssertionError.class, tester::test);

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        BadJava8TestSerdeProvider.class.getName()
                                + ": Provider not registered in META-INF/services. See"
                                + " https://www.creekservice.org/creek-kafka/#formats-on-the-class-path"));
    }
}
