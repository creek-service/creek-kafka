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

package org.creekservice.internal.kafka.streams.test.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.Set;
import org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClusterEndpointsProviderTest {

    @Mock private KafkaPropertyOverrides delegate;
    private ClusterEndpointsProvider provider;

    @BeforeEach
    void setUp() {
        provider = new ClusterEndpointsProvider(delegate);
    }

    @Test
    void shouldInitDelegate() {
        // Given:
        final Set<String> clusterNames = Set.of("a");

        // When:
        provider.init(clusterNames);

        // Then:
        verify(delegate).init(clusterNames);
    }

    @Test
    void shouldGetConfigFromDelegate() {
        // Given:
        doReturn(Map.of("a", "b")).when(delegate).get("cluster");

        // When:
        final Map<String, ?> result = provider.get("cluster");

        // Then:
        assertThat(result, is(Map.of("a", "b")));
    }

    @Test
    void shouldOverrideDelegate() {
        // Given:
        doReturn(Map.of("a", "b", "c", "d")).when(delegate).get("cluster");
        provider.put("cluster", Map.of("a", 10));

        // When:
        final Map<String, ?> result = provider.get("cluster");

        // Then:
        assertThat(result, is(Map.of("a", 10, "c", "d")));
    }
}
