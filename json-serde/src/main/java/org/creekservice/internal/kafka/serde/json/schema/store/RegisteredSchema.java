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

package org.creekservice.internal.kafka.serde.json.schema.store;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;

public final class RegisteredSchema<T> {

    private final ProducerSchema schema;
    private final int schemaId;
    private final String subject;
    private final Class<T> type;

    public RegisteredSchema(
            final ProducerSchema schema,
            final int schemaId,
            final String subject,
            final Class<T> type) {
        this.schema = requireNonNull(schema, "schema");
        this.schemaId = schemaId;
        this.subject = requireNonNull(subject, "subject");
        this.type = requireNonNull(type, "type");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RegisteredSchema<?> that = (RegisteredSchema<?>) o;
        return schemaId == that.schemaId
                && Objects.equals(schema, that.schema)
                && Objects.equals(subject, that.subject)
                && Objects.equals(type, that.type);
    }

    public int id() {
        return schemaId;
    }

    public ProducerSchema schema() {
        return schema;
    }

    public Class<T> type() {
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, schemaId, subject, type);
    }

    @Override
    public String toString() {
        return "RegisteredSchema{"
                + "schema="
                + schema
                + ", schemaId="
                + schemaId
                + ", subject='"
                + subject
                + '\''
                + ", type="
                + type
                + '}';
    }
}
