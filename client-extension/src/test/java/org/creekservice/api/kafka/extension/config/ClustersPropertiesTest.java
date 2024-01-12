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

package org.creekservice.api.kafka.extension.config;

import static org.creekservice.api.kafka.extension.config.ClustersProperties.propertiesBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClustersPropertiesTest {

    @Mock private KafkaPropertyOverrides provider;
    private ClustersProperties.Builder builder;

    @BeforeEach
    void setUp() {
        builder = propertiesBuilder();
    }

    @Test
    void shouldThrowOnNullPointers() {
        final NullPointerTester tester =
                new NullPointerTester()
                        .setDefault(String.class, "not empty")
                        .setDefault(KafkaPropertyOverrides.class, provider);

        tester.testAllPublicInstanceMethods(builder);
        tester.testAllPublicInstanceMethods(propertiesBuilder().build(Set.of()));
    }

    @Test
    void shouldImplementHashcodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        propertiesBuilder().put("a", "b", "c").putCommon("d", "e"),
                        propertiesBuilder().put("A", "b", "c").putCommon("d", "e"))
                .addEqualityGroup(propertiesBuilder().put("diff", "b", "c").putCommon("d", "e"))
                .addEqualityGroup(propertiesBuilder().put("a", "diff", "c").putCommon("d", "e"))
                .addEqualityGroup(propertiesBuilder().put("a", "b", "diff").putCommon("d", "e"))
                .addEqualityGroup(propertiesBuilder().put("a", "b", "c").putCommon("diff", "e"))
                .addEqualityGroup(propertiesBuilder().put("a", "b", "c").putCommon("d", "diff"))
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
        final ClustersProperties props = builder.put("cluster", "key", "value").build(Set.of());
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
                builder.put("cluster-bob", "k", 1).putCommon("k", 2).build(Set.of());

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
                propertiesBuilder().putCommon("k1", 2).putCommon("k2", 2).build(Set.of());

        // When:
        builder.putAll(other);

        // Then:
        final ClustersProperties props = builder.build(Set.of());
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
                propertiesBuilder().put("c", "k1", 2).put("c", "k2", 2).build(Set.of());

        // When:
        builder.putAll(other);

        // Then:
        final ClustersProperties props = builder.build(Set.of());
        assertThat(props.get("c").get("k1"), is(2));
        assertThat(props.get("c").get("k2"), is(2));
    }

    @Test
    void shouldChainPutAll() {
        // Given:
        final ClustersProperties other = propertiesBuilder().put("c", "k1", 2).build(Set.of());

        // When:
        builder.putAll(other).put("c", "k1", 1);

        // Then:
        final ClustersProperties props = builder.build(Set.of());
        assertThat(props.get("c").get("k1"), is(1));
    }

    @Test
    void shouldBeCaseInsensitiveOnClusterName() {
        // Given:
        final ClustersProperties props =
                propertiesBuilder()
                        .put("cluster", "k1", 1)
                        .put("cluster", "k2", 1)
                        .put("Cluster", "k1", 2)
                        .build(Set.of());

        // Then:
        assertThat(props.get("cLUSTer"), is(Map.of("k1", 2, "k2", 1)));
    }

    @Test
    void shouldGetProperties() {
        // Given:
        final ClustersProperties props =
                propertiesBuilder().putCommon("k1", 1).put("cluster", "k2", 2).build(Set.of());

        // Then:
        assertThat(Map.copyOf(props.properties("cluster")), is(Map.of("k1", 1, "k2", 2)));
    }

    @Test
    void shouldPassClusterNamesToOverridesProvider() {
        // Given:
        final ClustersProperties.Builder builder =
                propertiesBuilder().withOverridesProvider(provider);
        final Set<String> clusterNames = Set.of("a", "b");

        // When:
        builder.build(clusterNames);

        // Then:
        verify(provider).init(clusterNames);
    }

    @Test
    void shouldApplyOverrides() {
        // Given:
        final ClustersProperties props =
                propertiesBuilder()
                        .putCommon("common-config", "orig")
                        .putCommon("common-config-2", "orig")
                        .put("cluster-a", "specific-config", "orig")
                        .put("cluster-a", "specific-config-2", "orig")
                        .withOverridesProvider(provider)
                        .build(Set.of());

        doReturn(Map.of("common-config", "new-common", "specific-config", "new-specific"))
                .when(provider)
                .get("cluster-a");

        // Then:
        assertThat(
                props.get("cluster-a"),
                is(
                        Map.of(
                                "common-config",
                                "new-common",
                                "specific-config",
                                "new-specific",
                                "common-config-2",
                                "orig",
                                "specific-config-2",
                                "orig")));
    }
}
