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

package org.creekservice.api.kafka.streams.extension.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Objects;
import org.apache.kafka.streams.kstream.Named;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.Test;

class NameTest {

    private static final Name ROOT = Name.root();

    @Test
    void shouldNotPrefixRootNodes() {
        assertThat(ROOT.name("thing"), is("thing"));
        assertThat(ROOT.named("thing"), is(named("thing")));
    }

    @Test
    void shouldPrefixSubNodes() {
        // Given:
        final Name name = ROOT.postfix("sub");

        // Then:
        assertThat(name.name("thing"), is("sub.thing"));
        assertThat(name.named("thing"), is(named("sub.thing")));
    }

    @Test
    void shouldNestMultipleTimes() {
        // Given:
        final Name name = ROOT.postfix("a").postfix("b");

        // Then:
        assertThat(name.name("thing"), is("a.b.thing"));
        assertThat(name.named("thing"), is(named("a.b.thing")));
    }

    @Test
    void shouldSupportCustomDelimiter() {
        // Given:
        final Name name = Name.root('-').postfix("a").postfix("b");

        // Then:
        assertThat(name.name("thing"), is("a-b-thing"));
        assertThat(name.named("thing"), is(named("a-b-thing")));
    }

    private static Matcher<Named> named(final String expected) {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(final Named item, final Description mismatch) {
                final String actual = NamedAccessor.text(item);
                if (!Objects.equals(expected, actual)) {
                    mismatch.appendText("Expected: ")
                            .appendValue(expected)
                            .appendText(", but got: ")
                            .appendValue(actual);
                    return false;
                }

                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Named instance containing text: ").appendValue(expected);
            }
        };
    }
}
