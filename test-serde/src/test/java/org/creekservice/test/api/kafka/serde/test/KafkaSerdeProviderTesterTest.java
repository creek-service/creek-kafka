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

package org.creekservice.test.api.kafka.serde.test;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.creekservice.api.kafka.serde.test.KafkaSerdeProviderTester;
import org.creekservice.test.internal.kafka.serde.test.PrivateTestSerdeProvider;
import org.junit.jupiter.api.Test;

class KafkaSerdeProviderTesterTest {

    @Test
    void shouldPassIfRegisteredInBoth() {
        KafkaSerdeProviderTester.tester(PublicTestSerdeProvider.class).test();

        KafkaSerdeProviderTester.tester(PrivateTestSerdeProvider.class).test();
    }

    @Test
    void shouldFailIfNotRegisteredInModule() {
        // Given:
        final KafkaSerdeProviderTester tester =
                KafkaSerdeProviderTester.tester(OnlyMetaInfTestSerdeProvider.class);

        // When:
        final Error e = assertThrows(AssertionError.class, tester::test);

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        OnlyMetaInfTestSerdeProvider.class.getName()
                                + ": Provider not registered in module descriptor. See"
                                + " https://www.creekservice.org/creek-kafka/#formats-on-the-module-path"));
    }

    @Test
    void shouldFailIfNotRegisteredInMetaInf() {
        // Given:
        final KafkaSerdeProviderTester tester =
                KafkaSerdeProviderTester.tester(OnlyModuleTestSerdeProvider.class);

        // When:
        final Error e = assertThrows(AssertionError.class, tester::test);

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        OnlyModuleTestSerdeProvider.class.getName()
                                + ": Provider not registered in META-INF/services. See"
                                + " https://www.creekservice.org/creek-kafka/#formats-on-the-class-path"));
    }

    @Test
    void shouldFailIfRegisteredNeither() {
        // Given:
        final KafkaSerdeProviderTester tester =
                KafkaSerdeProviderTester.tester(NeitherTestSerdeProvider.class);

        // When:
        final Error e = assertThrows(AssertionError.class, tester::test);

        // Then:
        assertThat(e.getMessage(), containsString(NeitherTestSerdeProvider.class.getName()));
        assertThat(e.getMessage(), containsString("Provider not registered"));
    }

    @Test
    void shouldNotFailIfRegisteredNeitherIfChecksDisabled() {
        KafkaSerdeProviderTester.tester(NeitherTestSerdeProvider.class)
                .withoutTestingModulePath()
                .withoutTestingClassPath()
                .test();
    }

    @Test
    void shouldNotFailIfNotRegisteredInMetaInfServicesButCheckDisabled() {
        KafkaSerdeProviderTester.tester(OnlyModuleTestSerdeProvider.class)
                .withoutTestingClassPath()
                .test();
    }

    @Test
    void shouldFailOnUnexpectedFormat() {
        // Given:
        final KafkaSerdeProviderTester tester =
                KafkaSerdeProviderTester.tester(PublicTestSerdeProvider.class)
                        .withExpectedFormat(serializationFormat("diff"));

        // When:
        final Error e = assertThrows(AssertionError.class, tester::test);

        // Then:
        assertThat(e.getMessage(), containsString(PublicTestSerdeProvider.class.getName()));
        assertThat(
                e.getMessage(),
                containsString("unexpected format. expected: diff, actual: test-public"));
    }
}
