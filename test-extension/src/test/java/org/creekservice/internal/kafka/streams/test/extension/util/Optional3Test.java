/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.internal.kafka.streams.test.extension.util.Optional3.explicitlyNull;
import static org.creekservice.internal.kafka.streams.test.extension.util.Optional3.notProvided;
import static org.creekservice.internal.kafka.streams.test.extension.util.Optional3.of;
import static org.creekservice.internal.kafka.streams.test.extension.util.Optional3.ofNullable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class Optional3Test {

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(notProvided(), notProvided())
                .addEqualityGroup(explicitlyNull(), explicitlyNull())
                .addEqualityGroup(of(10), of(10))
                .addEqualityGroup(of(11))
                .testEquals();
    }

    @Test
    void shouldThrowNPE() throws Exception {
        new NullPointerTester()
                .ignore(Optional3.class.getMethod("ofNullable", Object.class))
                .testAllPublicStaticMethods(Optional3.class);
    }

    @Test
    void shouldReportProvided() {
        assertThat(notProvided().isProvided(), is(false));
        assertThat(explicitlyNull().isProvided(), is(true));
        assertThat(of("hi").isProvided(), is(true));
    }

    @Test
    void shouldReportPresent() {
        assertThat(notProvided().isPresent(), is(false));
        assertThat(explicitlyNull().isPresent(), is(false));
        assertThat(of("hi").isPresent(), is(true));
    }

    @Test
    void shouldGet() {
        assertThrows(NoSuchElementException.class, () -> notProvided().get());
        assertThat(explicitlyNull().get(), is(Optional.empty()));
        assertThat(of(8).get(), is(Optional.of(8)));
    }

    @Test
    void shouldMap() {
        assertThat(notProvided().map(Objects::toString), is(notProvided()));
        assertThat(explicitlyNull().map(Objects::toString), is(explicitlyNull()));
        assertThat(of(8).map(Objects::toString), is(of("8")));
    }

    @Test
    void shouldOrElse() {
        assertThat(notProvided().orElse(1, 2), is(2));
        assertThat(explicitlyNull().orElse(1, 2), is(1));
        assertThat(of(8).orElse(1, 2), is(8));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void shouldOrElseThrow() {
        assertThrows(NoSuchElementException.class, () -> notProvided().orElseThrow());
        assertThrows(NoSuchElementException.class, () -> explicitlyNull().orElseThrow());
        assertThat(of(8).orElseThrow(), is(8));
    }

    @Test
    void shouldToString() {
        assertThat(notProvided().toString(), is("<not-set>"));
        assertThat(explicitlyNull().toString(), is("null"));
        assertThat(of(8).toString(), is("8"));
    }

    @Test
    void shouldOfNullable() {
        assertThat(ofNullable(null), is(explicitlyNull()));
        assertThat(ofNullable(10), is(of(10)));
    }
}
