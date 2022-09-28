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

package org.creekservice.api.kafka.streams.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

class TestKafkaClientsExtensionOptionsTest {

    @Test
    void shouldSetStateDir() {
        // When:
        final Object stateDir =
                TestKafkaStreamsExtensionOptions.defaults()
                        .propertiesBuilder()
                        .build(Set.of())
                        .get("any")
                        .get(StreamsConfig.STATE_DIR_CONFIG);

        // Then:
        assertThat(stateDir, is(instanceOf(String.class)));
        assertThat(((String) stateDir).trim(), is(stateDir));
        assertThat(((String) stateDir).trim(), is(not("")));
    }
}
