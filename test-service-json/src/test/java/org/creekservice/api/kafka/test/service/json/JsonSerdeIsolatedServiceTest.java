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
package org.creekservice.api.kafka.test.service.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.ComponentOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.junit.jupiter.api.Test;

class JsonSerdeIsolatedServiceTest {

    @Test
    void shouldInitializeConsumerWithoutInitializingOwner() {
        final ServiceDescriptor consumer =
                service(List.of(JsonServiceDescriptor.OUTPUT.toInput()), List.of());
        try (CreekContext context = context(consumer)) {
            assertThat(
                    context.extension(KafkaStreamsExtension.class)
                            .topic(JsonServiceDescriptor.OUTPUT.toInput())
                            .valueSerde(),
                    notNullValue());
        }
    }

    @Test
    void shouldInitializeOwnedTopics() {
        try (CreekContext context = context(new JsonServiceDescriptor())) {
            assertThat(
                    context.extension(KafkaStreamsExtension.class)
                            .topic(JsonServiceDescriptor.OUTPUT)
                            .valueSerde(),
                    notNullValue());
        }
    }

    private static CreekContext context(final ServiceDescriptor service) {
        return CreekServices.builder(service)
                .with(JsonSerdeExtensionOptions.testBuilder().build())
                .with(KafkaStreamsExtensionOptions.testBuilder().build())
                .build();
    }

    private static ServiceDescriptor service(
            final List<ComponentInput> inputs, final List<ComponentOutput> outputs) {
        return new ServiceDescriptor() {
            @Override
            public String name() {
                return "isolated-consumer";
            }

            @Override
            public String dockerImage() {
                return "not used";
            }

            @Override
            public List<ComponentInput> inputs() {
                return inputs;
            }

            @Override
            public List<ComponentOutput> outputs() {
                return outputs;
            }
        };
    }
}
