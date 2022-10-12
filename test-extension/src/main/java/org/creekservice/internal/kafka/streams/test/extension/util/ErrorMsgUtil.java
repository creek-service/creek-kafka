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

package org.creekservice.internal.kafka.streams.test.extension.util;

import static java.lang.System.lineSeparator;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class ErrorMsgUtil {

    private ErrorMsgUtil() {}

    public static String formatList(final List<?> list) {
        return formatList(list, 0);
    }

    public static String formatList(final List<?> list, final int indentLevel) {
        final String indent = "\t".repeat(indentLevel + 1);
        final String finalIndent = "\t".repeat(indentLevel);
        return list.stream()
                .map(Objects::toString)
                .map(s -> lineSeparator() + indent + s)
                .collect(Collectors.joining(",", "[", lineSeparator() + finalIndent + "]"));
    }
}
