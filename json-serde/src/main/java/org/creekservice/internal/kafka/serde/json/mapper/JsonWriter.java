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
 * Two-stage writer of JSON.
 *
 * <p>Separating the writing into two stages allows for efficient validation against a schema.
 *
 * @param <T> the model type being written as JSON.
 */
public interface JsonWriter<T> {
    /**
     * Step one converts the model to a map of properties.
     *
     * @param model the model to convert.
     * @return the map of model properties.
     */
    Map<String, ?> convertToMap(T model);

    /**
     * Step two converts the map of properties to JSON bytes.
     *
     * @param properties the map of properties.
     * @return JSON data as bytes.
     */
    byte[] writeAsBytes(Map<String, ?> properties);
}
