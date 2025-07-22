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

package org.creekservice.api.kafka.serde.json.schema.store.endpoint;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * A mock {@link SchemaStoreEndpoints.Loader} interface to help with testing.
 *
 * <p>This mock interface defaults many of the base types methods, minimising the code changes
 * needed when upgrading Creek.
 *
 * <p>Register the mock during testing so that no external Schema Registry endpoint environment
 * variables are required. The mock is normally installed via {@link
 * org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions#testBuilder()}:
 *
 * <pre>
 * CreekServices.builder(new MyServiceDescriptor())
 *    .with(JsonSerdeExtensionOptions.testBuilder().build())
 *    .build();
 * </pre>
 *
 * <p>Or a custom client can be installed:
 *
 * <pre>
 * CreekServices.builder(new MyServiceDescriptor())
 *    .with(JsonSerdeExtensionOptions.builder()
 *        // Install custom store client:
 *        .withTypeOverride(JsonSchemaStoreClient.Factory.class, CustomStoreClient::new)
 *        // Install custom endpoint loading:
 *        .withTypeOverride(SchemaStoreEndpoints.Loader.class, new CustomStoreEndpointLoader()::load)
 *        .build())
 *    .build();
 * </pre>
 */
public interface MockEndpointsLoader extends SchemaStoreEndpoints.Loader {
    @Override
    default SchemaStoreEndpoints load(String schemaRegistryInstance) {
        return SchemaStoreEndpoints.create(List.of(URI.create("test-only")), Map.of());
    }
}
