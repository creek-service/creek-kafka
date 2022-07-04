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

package org.creekservice.api.kafka.common.config;

import static org.creekservice.api.kafka.common.config.ClustersProperties.propertiesBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClustersPropertiesTest {

    private ClustersProperties.Builder builder;

    @BeforeEach
    void setUp() {
        builder = propertiesBuilder();
    }

    @Test
    void shouldThrowOnNullPointers() {
        final NullPointerTester tester =
                new NullPointerTester().setDefault(String.class, "not empty");
        tester.testAllPublicInstanceMethods(builder);
        tester.testAllPublicInstanceMethods(propertiesBuilder().build());
    }

    @Test
    void shouldImplementHashcodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        propertiesBuilder().put("a", "b", "c").putCommon("d", "e").build(),
                        propertiesBuilder().put("a", "b", "c").putCommon("d", "e").build())
                .addEqualityGroup(
                        propertiesBuilder().put("diff", "b", "c").putCommon("d", "e").build())
                .addEqualityGroup(
                        propertiesBuilder().put("a", "diff", "c").putCommon("d", "e").build())
                .addEqualityGroup(
                        propertiesBuilder().put("a", "b", "diff").putCommon("d", "e").build())
                .addEqualityGroup(
                        propertiesBuilder().put("a", "b", "c").putCommon("diff", "e").build())
                .addEqualityGroup(
                        propertiesBuilder().put("a", "b", "c").putCommon("d", "diff").build())
                .testEquals();
    }

    @Test
    void shouldThrowOnBlankClusterName() {
        assertThrows(IllegalArgumentException.class, () -> builder.put(" ", "k", "v"));
    }

    @Test
    void shouldThrowOnBlankPropertyName() {
        assertThrows(IllegalArgumentException.class, () -> builder.put("c", "", "v"));
    }

    @Test
    void shouldNotExposeMutableProperties() {
        // Given:
        final ClustersProperties props = builder.put("cluster", "key", "value").build();
        final Map<String, ?> clusterProps = props.get("cluster");

        // When:
        clusterProps.put("mutate", null);

        // Then:
        assertThat(props.get("cluster"), not(hasKey("mutate")));
    }

    @Test
    void shouldUseMostSpecificProperty() {
        // Given:
        final ClustersProperties props =
                builder.put("cluster-bob", "k", 1).putCommon("k", 2).build();

        // When:
        final Object result = props.get("cluster-bob").get("k");

        // Then:
        assertThat(result, is(1));
    }

    @Test
    void shouldPutAllCommon() {
        // Given:
        builder.put("c", "k1", 1).putCommon("k2", 1);

        final ClustersProperties other =
                propertiesBuilder().putCommon("k1", 2).putCommon("k2", 2).build();

        // When:
        builder.putAll(other);

        // Then:
        final ClustersProperties props = builder.build();
        assertThat(props.get("any").get("k1"), is(2));
        assertThat(props.get("any").get("k2"), is(2));
        assertThat(props.get("c").get("k1"), is(1));
        assertThat(props.get("c").get("k2"), is(2));
    }

    @Test
    void shouldPutAllSpecific() {
        // Given:
        builder.put("c", "k1", 1);

        final ClustersProperties other =
                propertiesBuilder().put("c", "k1", 2).put("c", "k2", 2).build();

        // When:
        builder.putAll(other);

        // Then:
        final ClustersProperties props = builder.build();
        assertThat(props.get("c").get("k1"), is(2));
        assertThat(props.get("c").get("k2"), is(2));
    }

    @Test
    void shouldChainPutAll() {
        // Given:
        final ClustersProperties other = propertiesBuilder().put("c", "k1", 2).build();

        // When:
        builder.putAll(other).put("c", "k1", 1);

        // Then:
        final ClustersProperties props = builder.build();
        assertThat(props.get("c").get("k1"), is(1));
    }
}
