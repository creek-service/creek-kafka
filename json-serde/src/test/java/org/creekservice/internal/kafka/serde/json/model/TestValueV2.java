/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

/**
 * V1 -> V2
 *
 * <ul>
 *   <li>Removes optional {@code address} property
 *   <li>Puts back optional {@code name} string property, that was in v0
 *   <li>Adds new optional {@code v} int property
 * </ul>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@GeneratesSchema
public final class TestValueV2 {

    private final Optional<String> name;
    private final OptionalInt v;
    private final int age;

    public TestValueV2(
            @JsonProperty("age") final int age,
            @JsonProperty("name") final Optional<String> name,
            @JsonProperty("v") final OptionalInt v) {
        this.name = requireNonNull(name, "name");
        this.v = requireNonNull(v, "v");
        this.age = age;
    }

    @JsonGetter("name")
    public Optional<String> name() {
        return name;
    }

    @JsonGetter("v")
    public OptionalInt v() {
        return v;
    }

    @JsonGetter("age")
    @JsonSchemaInject(ints = @JsonSchemaInt(path = "minimum", value = 0))
    public int age() {
        return age;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TestValueV2 testValueV1 = (TestValueV2) o;
        return age == testValueV1.age
                && Objects.equals(name, testValueV1.name)
                && Objects.equals(v, testValueV1.v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age, v);
    }

    @Override
    public String toString() {
        return "TestModel{"
                + "name='"
                + name.orElse("<not-set>")
                + '\''
                + ", v="
                + (v.isPresent() ? v.getAsInt() : "<not-set>")
                + ", age="
                + age
                + '}';
    }
}
