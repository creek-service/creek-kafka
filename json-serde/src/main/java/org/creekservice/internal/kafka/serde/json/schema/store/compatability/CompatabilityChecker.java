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

package org.creekservice.internal.kafka.serde.json.schema.store.compatability;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.List;
import org.creekservice.api.kafka.serde.json.schema.ConsumerSchema;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient.VersionedSchema;

/**
 * Client-side check for compatability between a new proposed schema and existing schemas.
 *
 * <p>This compatability checker is requires because the default JSON Schema compatability checks in
 * the Confluent Schema Registry are broken. See the <a
 * href="https://www.creekservice.org/articles/2024/01/08/json-schema-evolution-part-1.html">Creek
 * post on this subject</a>.
 *
 * <p>This compatability checker checks that all schemas used to <i>consume</i> data are backwards
 * compatible with all schemas used to <i>produce</i> data.
 *
 * <p>Only producing schemas are registered in the schema registry. Producing schemas use a
 * <i>closed content model</i>.
 *
 * <p>Consume schemas use a <i>open content model</i>. Consuming schemas are not registered in the
 * schema registry. They are synthesised from a <i>registered</i> producing schemas.
 *
 * <p>Note: as this code is client-side, it has an inherent race condition. See <a
 * href="https://github.com/confluentinc/schema-registry/issues/2927">confluentinc/schema-registry/#2927</a>
 * for the issue to switch this code to server-side.
 */
public final class CompatabilityChecker {

    private final JsonSchemaStoreClient client;

    public CompatabilityChecker(final JsonSchemaStoreClient client) {
        this.client = requireNonNull(client, "client");
    }

    public void checkCompatability(final String subject, final ProducerSchema newProducerSchema) {
        final ConsumerSchema newConsumerSchema = newProducerSchema.toConsumerSchema();

        final List<VersionedSchema> versions = client.allVersions(subject);
        checkBackwardsCompatability(subject, newConsumerSchema, versions);
        checkForwardsCompatability(subject, newProducerSchema, versions);
    }

    private void checkForwardsCompatability(
            final String subject,
            final ProducerSchema producerSchema,
            final List<VersionedSchema> versions) {

        final JsonSchema newProducer = new JsonSchema(producerSchema.asJsonText());

        for (final VersionedSchema versioned : versions) {
            // Forward:
            // all documents that conform to the new schema are also valid according to the old
            final ConsumerSchema existingConsumer = versioned.schema().toConsumerSchema();
            final List<String> compatibleIssues =
                    new JsonSchema(existingConsumer.asJsonText()).isBackwardCompatible(newProducer);
            if (!compatibleIssues.isEmpty()) {
                throw new IncompatibleSchemaException(
                        "existing consumer schema "
                                + "is not compatible with proposed producer schema",
                        subject,
                        existingConsumer,
                        producerSchema,
                        versioned.version(),
                        compatibleIssues);
            }
        }
    }

    private void checkBackwardsCompatability(
            final String subject,
            final ConsumerSchema consumerSchema,
            final List<VersionedSchema> versions) {

        final JsonSchema newConsumer = new JsonSchema(consumerSchema.asJsonText());

        for (final VersionedSchema versioned : versions) {
            // Backwards:
            // all documents that conform to the old schema are also valid according to the new
            final ProducerSchema existingProducer = versioned.schema();
            final List<String> compatibleIssues =
                    newConsumer.isBackwardCompatible(new JsonSchema(existingProducer.asJsonText()));
            if (!compatibleIssues.isEmpty()) {
                throw new IncompatibleSchemaException(
                        "proposed consumer schema "
                                + "is not compatible with existing producer schema",
                        subject,
                        consumerSchema,
                        existingProducer,
                        versioned.version(),
                        compatibleIssues);
            }
        }
    }

    private static final class IncompatibleSchemaException extends RuntimeException {
        IncompatibleSchemaException(
                final String msg,
                final String subject,
                final ConsumerSchema consumerSchema,
                final ProducerSchema producerSchema,
                final int version,
                final List<String> compatibleIssues) {
            super(
                    msg
                            + ", subject: "
                            + subject
                            + ", version: "
                            + version
                            + ", issues: "
                            + compatibleIssues
                            + ", consumerSchema: "
                            + consumerSchema
                            + ", producingSchema: "
                            + producerSchema);
        }
    }
}
