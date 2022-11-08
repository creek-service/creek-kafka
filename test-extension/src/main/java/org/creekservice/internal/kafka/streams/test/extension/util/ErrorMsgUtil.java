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

/** Unit class for buiding error messages. */
public final class ErrorMsgUtil {

    private ErrorMsgUtil() {}

    /**
     * Format the list across multiple lines.
     *
     * @param list the list to format as a string.
     * @return the formatted string.
     */
    public static String formatList(final List<?> list) {
        return formatList(list, 0);
    }

    /**
     * Format the list across multiple lines using the supplied {@code indentLevel}.
     *
     * @param list the list to format as a string.
     * @param indentLevel the indent level to use.
     * @return the formatted string.
     */
    public static String formatList(final List<?> list, final int indentLevel) {
        final String indent = "\t".repeat(indentLevel + 1);
        final String finalIndent = "\t".repeat(indentLevel);
        return list.stream()
                .map(Objects::toString)
                .map(s -> lineSeparator() + indent + s)
                .collect(Collectors.joining(",", "[", lineSeparator() + finalIndent + "]"));
    }
}
