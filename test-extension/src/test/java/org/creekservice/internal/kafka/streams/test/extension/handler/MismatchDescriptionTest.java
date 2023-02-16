/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.internal.kafka.streams.test.extension.handler.MismatchDescription.mismatchDescription;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class MismatchDescriptionTest {

    @Test
    void shouldHandleNullExpected() {
        // When:
        final String text =
                mismatchDescription("path", Optional.empty(), Optional.of("45")).toString();

        // Then:
        assertThat(text, is("Mismatch@path@char0, expected: <empty>, actual: String(45)"));
    }

    @Test
    void shouldHandleNullActual() {
        // When:
        final String text =
                mismatchDescription("path", Optional.of(45L), Optional.empty()).toString();

        // Then:
        assertThat(text, is("Mismatch@path@char0, expected: Long(45), actual: <empty>"));
    }

    @Test
    void shouldWrapJdkTypesWithTypeInfo() {
        // When:
        final String text =
                mismatchDescription("path", Optional.of(45L), Optional.of("45")).toString();

        // Then:
        assertThat(text, is("Mismatch@path@char0, expected: Long(45), actual: String(45)"));
    }

    @Test
    void shouldNotWrapNonJdkTypes() {
        // When:
        final String text =
                mismatchDescription("path", Optional.of(new NonJdkType()), Optional.of(true))
                        .toString();

        // Then:
        assertThat(text, is("Mismatch@path@char0, expected: blah, actual: Boolean(true)"));
    }

    @Test
    void shouldMakeNewLinesAndCarriageReturnsExplicit() {
        assertThat(
                mismatchDescription(
                                "path",
                                Optional.of("line0\nline1\n"),
                                Optional.of("line0\r\nline1"))
                        .toString(),
                is(
                        "Mismatch@path@char13, expected: String(line0\\n"
                                + "line1\\n"
                                + "), actual: String(line0\\r"
                                + "\\n"
                                + "line1)"));
    }

    @Test
    void shouldNotSetDifferencePosIfParamsMatch() {
        assertThat(
                mismatchDescription("path", Optional.of(10), Optional.of(10)).toString(),
                is("Mismatch@path, expected: Integer(10), actual: Integer(10)"));
    }

    @Test
    void shouldHandleLongerExpected() {
        // When:
        final String text =
                mismatchDescription("path", Optional.of("longer"), Optional.of("long")).toString();

        // Then:
        assertThat(
                text, is("Mismatch@path@char11, expected: String(longer), actual: String(long)"));
    }

    @Test
    void shouldHandleLongerActual() {
        // When:
        final String text =
                mismatchDescription("path", Optional.of("long"), Optional.of("longer")).toString();

        // Then:
        assertThat(
                text, is("Mismatch@path@char11, expected: String(long), actual: String(longer)"));
    }

    private static final class NonJdkType {
        @Override
        public String toString() {
            return "blah";
        }
    }
}
