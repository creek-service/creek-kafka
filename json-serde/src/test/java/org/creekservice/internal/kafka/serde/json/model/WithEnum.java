/*
 * Copyright 2025 Creek Contributors (https://github.com/creek-service)
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
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@SuppressWarnings("unused")
@GeneratesSchema
public final class WithEnum {

    private final EnumType enumType;

    public enum EnumType {
        one,
        two,
        three
    }

    public WithEnum(@JsonProperty("enumType") final EnumType enumType) {
        this.enumType = enumType;
    }

    public EnumType getEnumType() {
        return enumType;
    }
}
