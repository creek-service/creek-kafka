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

package org.creekservice.test.kafka.test.java.eight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionProvider;
import org.creekservice.api.service.extension.CreekExtensionProvider;
import org.creekservice.api.service.extension.CreekExtensionProviders;
import org.junit.jupiter.api.Test;

class ServiceExtensionTest {

    @Test
    void shouldLoadStreamsExtension() {
        final List<CreekExtensionProvider<?>> found = CreekExtensionProviders.load();
        assertThat(
                found,
                containsInAnyOrder(
                        is(instanceOf(KafkaClientsExtensionProvider.class)),
                        is(instanceOf(KafkaStreamsExtensionProvider.class))));
    }
}
