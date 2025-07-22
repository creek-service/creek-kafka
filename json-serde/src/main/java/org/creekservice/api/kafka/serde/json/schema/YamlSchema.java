/*
 * Copyright 2024-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.serde.json.schema;

import org.creekservice.internal.kafka.serde.json.schema.YamlSchemas;

/** A JSON Schema, stored as YAML. */
public interface YamlSchema {

    /**
     * @return the schema converted to JSON.
     */
    String asJsonText();

    /**
     * @return the schema converted to Object.
     */
    default Object asObject() {
        return YamlSchemas.yamlToObject(toString());
    }
}
