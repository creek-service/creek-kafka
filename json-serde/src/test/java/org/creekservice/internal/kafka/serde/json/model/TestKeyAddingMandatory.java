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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

/** Incompatible as it adds a {@code newMandatory} property. */
@GeneratesSchema
public class TestKeyAddingMandatory {

    private final int id;
    private final int newMandatory;

    public TestKeyAddingMandatory(
            @JsonProperty("id") final int id,
            @JsonProperty("newMandatory") final int newMandatory) {
        this.id = id;
        this.newMandatory = newMandatory;
    }

    @JsonGetter("id")
    @JsonSchemaInject(ints = @JsonSchemaInt(path = "minimum", value = 0))
    public int id() {
        return id;
    }

    @JsonGetter("newMandatory")
    public int newMandatory() {
        return newMandatory;
    }
}
