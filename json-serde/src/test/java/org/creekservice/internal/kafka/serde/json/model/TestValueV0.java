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
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@GeneratesSchema
public final class TestValueV0 {

    private final Optional<String> name;
    private final int age;

    public TestValueV0(
            @JsonProperty("name") final Optional<String> name, @JsonProperty("age") final int age) {
        this.name = requireNonNull(name, "name");
        this.age = age;
    }

    @JsonGetter("name")
    public Optional<String> name() {
        return name;
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
        final TestValueV0 testValueV1 = (TestValueV0) o;
        return age == testValueV1.age && Objects.equals(name, testValueV1.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age);
    }

    @Override
    public String toString() {
        return "TestModel{" + "name='" + name.orElse("<not-set>") + '\'' + ", age=" + age + '}';
    }
}
