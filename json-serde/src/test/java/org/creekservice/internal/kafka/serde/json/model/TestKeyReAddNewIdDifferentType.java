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
public class TestKeyReAddNewIdDifferentType {

    private final int id;
    private final Optional<String> newId;

    public TestKeyReAddNewIdDifferentType(
            @JsonProperty("id") final int id, @JsonProperty("newId") final Optional<String> newId) {
        this.id = id;
        this.newId = requireNonNull(newId, "newId");
    }

    @JsonGetter("id")
    @JsonSchemaInject(ints = @JsonSchemaInt(path = "minimum", value = 0))
    public int id() {
        return id;
    }

    @JsonGetter("newId")
    public Optional<String> newId() {
        return newId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TestKeyReAddNewIdDifferentType testKeyV1 = (TestKeyReAddNewIdDifferentType) o;
        return id == testKeyV1.id && Objects.equals(newId, testKeyV1.newId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, newId);
    }

    @Override
    public String toString() {
        return "TestKey{" + "id=" + id + "newId=" + newId.orElse("<not-set>") + '}';
    }
}
