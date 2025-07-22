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

package org.creekservice.api.kafka.metadata.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.junit.jupiter.api.Test;

class SchemaDescriptorTest {

    @Test
    void shouldDefaultClusterName() {
        // Given:
        final SchemaDescriptor<?> descriptor =
                new SchemaDescriptor<>() {
                    @Override
                    public KafkaTopicDescriptor.PartDescriptor<Object> part() {
                        return null;
                    }

                    @Override
                    public URI id() {
                        return null;
                    }
                };

        // Then:
        assertThat(
                descriptor.schemaRegistryName(), is(SchemaDescriptor.DEFAULT_SCHEMA_REGISTRY_NAME));
    }
}
