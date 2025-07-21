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

package org.creekservice.internal.kafka.serde.json.mapper;

import java.util.Map;

/**
 * Two-stage reader of JSON.
 *
 * <p>Separating the reading into two stages allows for efficient validation against a schema.
 *
 * @param <T> the model type being read as JSON.
 */
public interface JsonReader<T> {
    /**
     * Step one converts the raw JSON to a map of properties.
     *
     * @param data the data to convert
     * @return the map of model properties.
     */
    Map<String, Object> readValue(byte[] data);

    /**
     * Step two converts the map of properties to the model type.
     *
     * @param properties the map of model properties.
     * @return the deserialized model.
     */
    T convertFromMap(Map<String, ?> properties);
}
