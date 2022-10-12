/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler.ExpectationOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TestOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestOptionsAccessorTest {

    @Mock private ExpectationOptions options;
    @Mock private TestOptions testOptions;

    @Test
    void shouldReturnDefaultOptions() {
        // When:
        final TestOptions result = TestOptionsAccessor.get(options);

        // Then:
        assertThat(result, is(TestOptions.defaults()));
    }

    @Test
    void shouldReturnUserSuppliedOptions() {
        // Given:
        when(options.get(TestOptions.class)).thenReturn(List.of(testOptions));

        // When:
        final TestOptions result = TestOptionsAccessor.get(options);

        // Then:
        assertThat(result, is(testOptions));
    }

    @Test
    void shouldThrowOnDuplicateOptions() {
        // Given:
        when(options.get(TestOptions.class)).thenReturn(List.of(testOptions, testOptions));

        when(testOptions.location())
                .thenReturn(URI.create("file:///loc0"), URI.create("file:///loc1"));

        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class, () -> TestOptionsAccessor.get(options));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Test suite should only define single 'creek/kafka-options@1' option. "
                                + "locations: [file:///loc0, file:///loc1]"));
    }
}
