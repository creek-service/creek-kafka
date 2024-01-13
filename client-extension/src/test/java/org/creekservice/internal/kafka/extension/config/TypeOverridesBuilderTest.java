/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.NullPointerTester;
import java.util.Optional;
import org.creekservice.api.kafka.extension.config.TypeOverrides;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TypeOverridesBuilderTest {

    private TypeOverridesBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new TypeOverridesBuilder();
    }

    @Test
    void shouldThrowNPEs() {
        final NullPointerTester tester =
                new NullPointerTester().setDefault(Class.class, String.class);

        tester.testAllPublicInstanceMethods(builder);
        tester.testAllPublicInstanceMethods(builder.build());
    }

    @Test
    void shouldReturnEmptyIfNoOverride() {
        // Given:
        builder.set(String.class, "this");
        final TypeOverrides overrides = builder.build();

        // Then:
        assertThat(overrides.get(Long.class), is(Optional.empty()));
    }

    @Test
    void shouldNotFindSuperTypeIfLookingUpBySubType() {
        // Given:
        builder.set(Number.class, 1L);
        final TypeOverrides overrides = builder.build();

        // Then:
        assertThat(overrides.get(Long.class), is(Optional.empty()));
    }

    @Test
    void shouldSetOverride() {
        // When:
        builder.set(String.class, "this");

        // Then:
        assertThat(builder.build().get(String.class), is(Optional.of("this")));
    }

    @Test
    void shouldOverrideAnyCurrentOverride() {
        // Given:
        builder.set(String.class, "this");

        // When:
        builder.set(String.class, "that");

        // Then:
        assertThat(builder.build().get(String.class), is(Optional.of("that")));
    }
}
