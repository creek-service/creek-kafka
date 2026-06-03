/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import java.time.Duration;
import java.util.Objects;
import org.creekservice.api.system.test.extension.component.definition.ServiceDefinition;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;

final class SchemaRegistryContainerDef implements ServiceDefinition {

    public static final int SCHEMA_REGISTRY_PORT = 8081;
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);

    private final String name;
    private final String dockerImage;
    private final ServiceInstance kafkaInstance;

    SchemaRegistryContainerDef(
            final String registryName,
            final String dockerImage,
            final ServiceInstance kafkaInstance) {
        this.name = "schema-registry-" + requireNonBlank(registryName, "registryName");
        this.dockerImage = requireNonBlank(dockerImage, "dockerImage");
        this.kafkaInstance = requireNonNull(kafkaInstance, "kafkaInstance");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String dockerImage() {
        return dockerImage;
    }

    @Override
    public void configureInstance(final ConfigurableServiceInstance instance) {
        instance.addExposedPorts(SCHEMA_REGISTRY_PORT)
                .addEnv("SCHEMA_REGISTRY_HOST_NAME", name)
                .addEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                .addEnv(
                        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        "PLAINTEXT://"
                                + kafkaInstance.name()
                                + ":"
                                + KafkaContainerDef.SERVICE_NETWORK_PORT)
                .addEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "full_transitive")
                .setStartupTimeout(STARTUP_TIMEOUT)
                .setStartupLogMessage(".*Server started, listening for requests.*", 1);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SchemaRegistryContainerDef that = (SchemaRegistryContainerDef) o;
        return Objects.equals(name, that.name)
                && Objects.equals(dockerImage, that.dockerImage)
                && Objects.equals(kafkaInstance, that.kafkaInstance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dockerImage, kafkaInstance);
    }
}
