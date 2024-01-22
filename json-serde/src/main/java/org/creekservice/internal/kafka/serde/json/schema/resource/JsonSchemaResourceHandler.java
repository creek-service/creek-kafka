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

package org.creekservice.internal.kafka.serde.json.schema.resource;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.creekservice.api.base.type.CodeLocation.codeLocation;

import java.util.Collection;
import java.util.List;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.OwnedJsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.SchemaDescriptor;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.api.service.extension.component.model.ResourceHandler;
import org.creekservice.internal.kafka.serde.json.schema.store.SchemaStore;
import org.creekservice.internal.kafka.serde.json.schema.store.SrSchemaStores;

public final class JsonSchemaResourceHandler implements ResourceHandler<JsonSchemaDescriptor<?>> {

    private final SrSchemaStores schemaStores;
    private final SchemaResourceValidator validator;
    private final StructuredLogger logger;

    public JsonSchemaResourceHandler(final SrSchemaStores schemaStores) {
        this(
                schemaStores,
                new SchemaResourceValidator(),
                StructuredLoggerFactory.internalLogger(JsonSchemaResourceHandler.class));
    }

    @VisibleForTesting
    JsonSchemaResourceHandler(
            final SrSchemaStores schemaStores,
            final SchemaResourceValidator validator,
            final StructuredLogger logger) {
        this.schemaStores = requireNonNull(schemaStores, "schemaStores");
        this.validator = requireNonNull(validator, "validator");
        this.logger = requireNonNull(logger, "logger");
    }

    @Override
    public void validate(final Collection<? extends JsonSchemaDescriptor<?>> resources) {
        validator.validateGroup(resources);
    }

    @Override
    public void ensure(final Collection<? extends JsonSchemaDescriptor<?>> schemas) {
        schemas.stream()
                .map(JsonSchemaResourceHandler::toCreatable)
                .collect(groupingBy(JsonSchemaDescriptor::schemaRegistryName))
                .forEach(this::ensure);
    }

    @Override
    public void prepare(final Collection<? extends JsonSchemaDescriptor<?>> resources) {
        // No-op.
        // Schemas are read during preparation of the topics they belong to.
    }

    private void ensure(
            final String schemaRegistryName,
            final List<? extends OwnedJsonSchemaDescriptor<?>> schema) {
        logger.debug(
                "Ensuring topic schemas",
                log ->
                        log.with(
                                "schema-ids",
                                schema.stream().map(ResourceDescriptor::id).collect(toList())));

        final SchemaStore schemaStore = schemaStores.get(schemaRegistryName);
        schema.stream().map(SchemaDescriptor::part).forEach(schemaStore::registerFromClasspath);
    }

    private static OwnedJsonSchemaDescriptor<?> toCreatable(final JsonSchemaDescriptor<?> schema) {
        if (schema instanceof OwnedJsonSchemaDescriptor) {
            return (OwnedJsonSchemaDescriptor<?>) schema;
        }
        throw new IllegalArgumentException(
                "Schema descriptor is not creatable: schema-id: "
                        + schema.id()
                        + ", schema: "
                        + schema
                        + " ("
                        + codeLocation(schema)
                        + ")");
    }
}
