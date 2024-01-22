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

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaRegistryEndpoint;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final int PORT = 8081;

    @SuppressWarnings("resource")
    public SchemaRegistryContainer(final DockerImageName imageName, final KafkaContainer kafka) {
        super(imageName);

        withExposedPorts(PORT)
                .withNetwork(kafka.getNetwork())
                .withNetworkAliases("schema-registry")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + PORT)
                .withEnv(
                        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        "PLAINTEXT://" + kafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "full_transitive")
                .dependsOn(kafka)
                .setWaitStrategy(new HostPortWaitStrategy());
    }

    @Override
    public void start() {
        super.start();
    }

    public SchemaRegistryEndpoint clientEndpoint() {
        try {
            return SchemaRegistryEndpoint.create(
                    List.of(URI.create("http://localhost:" + getMappedPort(PORT))), Map.of());
        } catch (Exception e) {
            throw new RuntimeException("failed to create endpoint", e);
        }
    }
}
