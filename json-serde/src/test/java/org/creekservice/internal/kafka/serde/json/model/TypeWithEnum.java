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
import java.util.Objects;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@GeneratesSchema
public final class TypeWithEnum {

    public enum Colour {
        red,
        blue,
        yellow
    }

    private final Colour colour;

    public TypeWithEnum(@JsonProperty("colour") final Colour colour) {
        this.colour = requireNonNull(colour, "colour");
    }

    @JsonGetter("colour")
    public Colour colour() {
        return colour;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TypeWithEnum testValueV1 = (TypeWithEnum) o;
        return Objects.equals(colour, testValueV1.colour);
    }

    @Override
    public int hashCode() {
        return Objects.hash(colour);
    }

    @Override
    public String toString() {
        return "TestValueWithEnum{" + "colour=" + colour + '}';
    }
}
