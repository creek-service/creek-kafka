/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.StringJoiner;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
final class MismatchDescription {

    private final String path;
    private final String expected;
    private final String actual;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static MismatchDescription mismatchDescription(
            final String path, final Optional<?> expected, final Optional<?> actual) {
        return new MismatchDescription(path, toValueAndType(expected), toValueAndType(actual));
    }

    private static String toValueAndType(final Optional<?> value) {
        return value.map(MismatchDescription::toValueAndType).orElse("<empty>");
    }

    private static String toValueAndType(final Object value) {
        final String text =
                value.getClass().getPackageName().startsWith("java")
                        ? value.getClass().getSimpleName() + "(" + value + ")"
                        : value.toString();

        // Make new line characters explicit:
        return text.replaceAll("\r", "\\\\r").replaceAll("\n", "\\\\n");
    }

    private MismatchDescription(final String path, final String expected, final String actual) {
        this.path = requireNonNull(path, "path");
        this.expected = requireNonNull(expected, "expected");
        this.actual = requireNonNull(actual, "actual");
    }

    @Override
    public String toString() {
        final int diffPos = getPositionWhereTextDiffer(expected, actual);
        final String charPos = diffPos == -1 ? "" : "@char" + diffPos;

        return new StringJoiner(", ")
                .add("Mismatch@" + path + charPos)
                .add("expected: " + expected)
                .add("actual: " + actual)
                .toString();
    }

    private static int getPositionWhereTextDiffer(final String a, final String b) {
        int position = 0;
        while (b.length() > position
                && a.length() > position
                && a.charAt(position) == b.charAt(position)) {
            position++;
        }
        return position == Math.max(a.length(), b.length()) ? -1 : position;
    }
}
