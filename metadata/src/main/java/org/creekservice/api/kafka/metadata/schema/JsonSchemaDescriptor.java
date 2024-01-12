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

package org.creekservice.api.kafka.metadata.schema;

import java.net.URI;

/**
 * Describes a JSON Schema resource.
 *
 * @param <T> the type the schema represents.
 */
public interface JsonSchemaDescriptor<T> extends SchemaDescriptor<T> {

    @Override
    default URI id() {
        return SchemaDescriptor.resourceId(schemaRegistryName(), part());
    }
}
