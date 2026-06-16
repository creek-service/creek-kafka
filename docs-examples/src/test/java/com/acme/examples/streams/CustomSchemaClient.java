package com.acme.examples.streams;

import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.util.List;

@SuppressWarnings("unused")
public class CustomSchemaClient implements JsonSchemaStoreClient {
    public CustomSchemaClient(final String schemaRegistryName, final SchemaRegistryClient client) {
    }

    @Override
    public void disableCompatability(final String subject) {

    }

    @Override
    public int register(final String subject, final ProducerSchema schema) {
        return 0;
    }

    @Override
    public int registeredId(final String subject, final ProducerSchema schema) {
        return 0;
    }

    @Override
    public List<VersionedSchema> allVersions(final String subject) {
        return List.of();
    }
}
