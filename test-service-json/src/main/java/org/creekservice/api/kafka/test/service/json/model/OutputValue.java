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

package org.creekservice.api.kafka.test.service.json.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@GeneratesSchema
public final class OutputValue {

    private final String key;
    private final long value;

    public OutputValue(
            @JsonProperty(value = "key", required = true) final String key,
            @JsonProperty("value") final long value) {
        this.key = requireNonNull(key, "key");
        this.value = value;
    }

    public Optional<String> getKey() {
        return Optional.of(key);
    }

    public long getValue() {
        return value;
    }
}
