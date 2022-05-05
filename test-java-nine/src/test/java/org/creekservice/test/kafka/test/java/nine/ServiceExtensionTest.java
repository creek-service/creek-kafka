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

package org.creekservice.test.kafka.test.java.nine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import java.util.List;
import java.util.stream.Collectors;
import org.creekservice.api.service.extension.CreekExtensionBuilder;
import org.creekservice.api.service.extension.CreekExtensions;
import org.junit.jupiter.api.Test;

class ServiceExtensionTest {

    @Test
    void shouldLoadStreamsExtension() {
        // When:
        final List<CreekExtensionBuilder> found = CreekExtensions.load();

        // Then:
        final List<String> classNames =
                found.stream()
                        .map(Object::getClass)
                        .map(Class::getName)
                        .collect(Collectors.toUnmodifiableList());
        assertThat(
                classNames,
                hasItem(
                        "org.creekservice.internal.kafka.streams.extension.KafkaStreamsExtensionBuilder"));
    }
}
