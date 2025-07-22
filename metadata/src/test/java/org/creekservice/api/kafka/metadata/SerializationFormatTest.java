/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.metadata;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.EqualsTester;
import org.junit.jupiter.api.Test;

class SerializationFormatTest {

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(serializationFormat("test"), serializationFormat("test"))
                .addEqualityGroup(serializationFormat("diff"))
                .testEquals();
    }

    @Test
    void shouldImplementToString() {
        assertThat(serializationFormat("test").toString(), is("test"));
    }

    @Test
    void shouldThrowIfNull() {
        assertThrows(NullPointerException.class, () -> serializationFormat(null));
    }

    @Test
    void shouldThrowIfEmpty() {
        // When:
        final Exception e =
                assertThrows(IllegalArgumentException.class, () -> serializationFormat(""));

        // Then:
        assertThat(e.getMessage(), is("name can not be blank"));
    }

    @Test
    void shouldThrowIfJustWhiteSpace() {
        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class, () -> serializationFormat(" \t \n \r "));

        // Then:
        assertThat(e.getMessage(), is("name can not be blank"));
    }

    @Test
    void shouldThrowIfNeedsTrimming() {
        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> serializationFormat("I need a trim\t"));

        // Then:
        assertThat(e.getMessage(), is("name should be trimmed: 'I need a trim\t'"));
    }
}
