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

package org.creekservice.api.kafka.metadata;

import static org.creekservice.api.kafka.metadata.KafkaResourceIds.topicId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;

class KafkaResourceIdsTest {

    @Test
    void shouldCreateUniqueKafkaTopicResourceId() {
        assertThat(
                topicId("cluster-a", "topic-b").toString(), is("kafka-topic://cluster-a/topic-b"));
    }

    @Test
    void shouldThrowOnInvalidId() {
        // When:
        final Exception e =
                assertThrows(IllegalArgumentException.class, () -> topicId("%%%%", "%%%%"));

        // Then:
        assertThat(e.getCause(), instanceOf(URISyntaxException.class));
        assertThat(e.getMessage(), containsString("Malformed escape pair"));
    }
}
