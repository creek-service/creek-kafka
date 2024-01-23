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

package org.creekservice.internal.kafka.serde.json;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.CodeLocation.codeLocation;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.serde.JsonSchemaKafkaSerde;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.component.model.ComponentModelContainer.HandlerTypeRef;
import org.creekservice.internal.kafka.serde.json.logging.LoggingField;
import org.creekservice.internal.kafka.serde.json.mapper.GenericMapper;
import org.creekservice.internal.kafka.serde.json.mapper.GenericMapperFactory;
import org.creekservice.internal.kafka.serde.json.schema.SchemaConvertor;
import org.creekservice.internal.kafka.serde.json.schema.resource.JsonSchemaResourceHandler;
import org.creekservice.internal.kafka.serde.json.schema.serde.JsonSchemaSerde;
import org.creekservice.internal.kafka.serde.json.schema.store.RegisteredSchema;
import org.creekservice.internal.kafka.serde.json.schema.store.SchemaStore;
import org.creekservice.internal.kafka.serde.json.schema.store.SrSchemaStores;
import org.creekservice.internal.kafka.serde.json.schema.store.client.DefaultJsonSchemaRegistryClient;
import org.creekservice.internal.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.internal.kafka.serde.json.schema.validation.SchemaFriendValidator;
import org.creekservice.internal.kafka.serde.json.schema.validation.SchemaValidator;

public class JsonSchemaSerdeProvider implements KafkaSerdeProvider {

    private final JsonSchemaStoreClient.Factory defaultStoreClientFactory;
    private final SchemaStoresFactory schemaStoresFactory;

    public JsonSchemaSerdeProvider() {
        this(DefaultJsonSchemaRegistryClient::new, SrSchemaStores::new);
    }

    @VisibleForTesting
    JsonSchemaSerdeProvider(
            final JsonSchemaStoreClient.Factory defaultStoreClientFactory,
            final SchemaStoresFactory schemaStoresFactory) {
        this.defaultStoreClientFactory =
                requireNonNull(defaultStoreClientFactory, "defaultStoreClientFactory");
        this.schemaStoresFactory = requireNonNull(schemaStoresFactory, "schemaStoreFactory");
    }

    @Override
    public SerializationFormat format() {
        return JsonSchemaKafkaSerde.format();
    }

    @Override
    public JsonSerdeFactory initialize(final CreekService api) {
        final JsonSerdeExtensionOptions options =
                api.options()
                        .get(JsonSerdeExtensionOptions.class)
                        .orElseGet(() -> JsonSerdeExtensionOptions.builder().build());

        final JsonSchemaStoreClient.Factory clientFactory =
                options.typeOverride(JsonSchemaStoreClient.Factory.class)
                        .orElse(defaultStoreClientFactory);

        final SrSchemaStores schemaStores =
                schemaStoresFactory.create(
                        schemaRegistryName ->
                                clientFactory.create(schemaRegistryName, options::typeOverride));

        api.components()
                .model()
                .addResource(
                        new HandlerTypeRef<>() {}, new JsonSchemaResourceHandler(schemaStores));

        return new JsonSerdeFactory(
                schemaStores,
                SchemaFriendValidator::new,
                new GenericMapperFactory(options.subTypes()),
                JsonSchemaSerde::new,
                StructuredLoggerFactory.internalLogger(JsonSchemaSerdeProvider.class));
    }

    @VisibleForTesting
    static final class JsonSerdeFactory implements KafkaSerdeProvider.SerdeFactory {

        private final SrSchemaStores schemaStores;
        private final StructuredLogger logger;
        private final ValidatorFactory validatorFactory;
        private final GenericMapperFactory mapperFactory;
        private final SerdeFactory jsonSchemaSerdeFactory;

        JsonSerdeFactory(
                final SrSchemaStores schemaStores,
                final ValidatorFactory validatorFactory,
                final GenericMapperFactory mapperFactory,
                final SerdeFactory jsonSchemaSerdeFactory,
                final StructuredLogger logger) {
            this.schemaStores = requireNonNull(schemaStores, "schemaStores");
            this.validatorFactory = requireNonNull(validatorFactory, "validatorFactory");
            this.mapperFactory = requireNonNull(mapperFactory, "mapperFactory");
            this.jsonSchemaSerdeFactory =
                    requireNonNull(jsonSchemaSerdeFactory, "jsonSchemaSerdeFactory");
            this.logger = requireNonNull(logger, "logger");
        }

        @Override
        public <T> Serde<T> createSerde(final PartDescriptor<T> part) {
            final SchemaStore schemaStore = schemaStores.get(schemaRegistryName(part));
            final RegisteredSchema<T> schema = schemaStore.loadFromClasspath(part);

            logger.debug(
                    "Building JSON Schema serde",
                    log ->
                            log.with(LoggingField.part, part.name())
                                    .with(LoggingField.topicId, part.topic().id())
                                    .with(LoggingField.schemaId, schema.id()));

            final SchemaValidator producerValidator = validatorFactory.create(schema.schema());
            final SchemaValidator consumerValidator =
                    validatorFactory.create(SchemaConvertor.toConsumerSchema(schema.schema()));
            final GenericMapper<T> mapper = mapperFactory.create(part.type());
            return jsonSchemaSerdeFactory.create(consumerValidator, producerValidator, mapper);
        }

        private static String schemaRegistryName(final PartDescriptor<?> part) {
            return part.resources()
                    .filter(JsonSchemaDescriptor.class::isInstance)
                    .map(JsonSchemaDescriptor.class::cast)
                    .findFirst()
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "Part is not associated with a JSON schema. topic: "
                                                    + part.topic().id()
                                                    + ", part: "
                                                    + part.name()
                                                    + " ("
                                                    + codeLocation(part)
                                                    + ")"))
                    .schemaRegistryName();
        }
    }

    @VisibleForTesting
    interface SchemaStoresFactory {
        SrSchemaStores create(SrSchemaStores.ClientFactory clientFactory);
    }

    @VisibleForTesting
    interface ValidatorFactory {
        SchemaValidator create(JsonSchema schema);
    }

    @VisibleForTesting
    interface SerdeFactory {
        <T> Serde<T> create(
                SchemaValidator consumerValidator,
                SchemaValidator producerValidator,
                GenericMapper<T> mapper);
    }
}
