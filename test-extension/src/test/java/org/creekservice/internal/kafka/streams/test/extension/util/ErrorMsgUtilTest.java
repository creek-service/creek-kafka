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

package org.creekservice.internal.kafka.streams.test.extension.util;

import static java.lang.System.lineSeparator;
import static org.creekservice.internal.kafka.streams.test.extension.util.ErrorMsgUtil.formatList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.junit.jupiter.api.Test;

class ErrorMsgUtilTest {

    @Test
    void shouldFormatList() {
        // Given:
        final List<?> list = List.of("hello", 22, 19L);
        assertThat(
                formatList(list),
                is(
                        "["
                                + lineSeparator()
                                + "\thello,"
                                + lineSeparator()
                                + "\t22,"
                                + lineSeparator()
                                + "\t19"
                                + lineSeparator()
                                + "]"));
    }

    @Test
    void shouldFormatListWithIndent() {
        // Given:
        final List<?> list = List.of("hello", 22, 19L);
        assertThat(
                formatList(list, 2),
                is(
                        "["
                                + lineSeparator()
                                + "\t\t\thello,"
                                + lineSeparator()
                                + "\t\t\t22,"
                                + lineSeparator()
                                + "\t\t\t19"
                                + lineSeparator()
                                + "\t\t]"));
    }
}
