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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SchemaRegistryContainerDefTest {

    private static final String REGISTRY_NAME = "my-registry";
    private static final String DOCKER_IMAGE = "confluentinc/cp-schema-registry:7.5.0";

    @Mock(answer = Answers.RETURNS_SELF)
    private ConfigurableServiceInstance instance;

    @Mock private ServiceInstance kafkaInstance;

    private SchemaRegistryContainerDef def;

    @BeforeEach
    void setUp() {
        when(kafkaInstance.name()).thenReturn("kafka-default-0");
        def = new SchemaRegistryContainerDef(REGISTRY_NAME, DOCKER_IMAGE, kafkaInstance);
    }

    @Test
    void shouldReturnPrefixedName() {
        assertThat(def.name(), is("schema-registry-" + REGISTRY_NAME));
    }

    @Test
    void shouldReturnDockerImage() {
        assertThat(def.dockerImage(), is(DOCKER_IMAGE));
    }

    @Test
    void shouldExposeSchemaRegistryPort() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addExposedPorts(SchemaRegistryContainerDef.SCHEMA_REGISTRY_PORT);
    }

    @Test
    void shouldSetHostnameEnvVar() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry-" + REGISTRY_NAME);
    }

    @Test
    void shouldSetListenersEnvVar() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance)
                .addEnv(
                        "SCHEMA_REGISTRY_LISTENERS",
                        "http://0.0.0.0:" + SchemaRegistryContainerDef.SCHEMA_REGISTRY_PORT);
    }

    @Test
    void shouldSetBootstrapServersEnvVar() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance)
                .addEnv(
                        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        "PLAINTEXT://kafka-default-0:" + KafkaContainerDef.SERVICE_NETWORK_PORT);
    }

    @Test
    void shouldSetSchemaCompatibilityEnvVar() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "full_transitive");
    }

    @Test
    void shouldBeEqualWhenAllFieldsMatch() {
        // Given:
        final SchemaRegistryContainerDef other =
                new SchemaRegistryContainerDef(REGISTRY_NAME, DOCKER_IMAGE, kafkaInstance);

        // Then:
        assertThat(def, is(other));
        assertThat(def.hashCode(), is(other.hashCode()));
    }

    @Test
    void shouldNotBeEqualWhenRegistryNameDiffers() {
        final SchemaRegistryContainerDef other =
                new SchemaRegistryContainerDef("other-registry", DOCKER_IMAGE, kafkaInstance);

        assertThat(def, is(not(other)));
    }

    @Test
    void shouldNotBeEqualWhenDockerImageDiffers() {
        final SchemaRegistryContainerDef other =
                new SchemaRegistryContainerDef(REGISTRY_NAME, "other-image:1.0", kafkaInstance);

        assertThat(def, is(not(other)));
    }

    @Test
    void shouldNotBeEqualWhenKafkaInstanceDiffers() {
        final ServiceInstance otherKafka = mock(ServiceInstance.class);
        final SchemaRegistryContainerDef other =
                new SchemaRegistryContainerDef(REGISTRY_NAME, DOCKER_IMAGE, otherKafka);

        assertThat(def, is(not(other)));
    }

    @Test
    void shouldBeEqualToSelf() {
        assertThat(def, is(def));
    }
}
