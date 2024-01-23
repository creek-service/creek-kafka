/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@GeneratesSchema
public final class WithAmbiguousFloat {

    private final Object number;

    public WithAmbiguousFloat(@JsonProperty("number") final Object number) {
        this.number = number;
    }

    @JsonSchemaInject(json = "{\"type\": \"number\"}")
    public Object getNumber() {
        return number;
    }
}
