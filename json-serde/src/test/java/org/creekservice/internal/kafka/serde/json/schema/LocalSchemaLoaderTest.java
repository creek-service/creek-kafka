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

package org.creekservice.internal.kafka.serde.json.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URL;
import java.nio.file.Path;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.test.service.json.model.OutputValue;
import org.creekservice.api.test.util.TestPaths;
import org.junit.jupiter.api.Test;

class LocalSchemaLoaderTest {

    @Test
    public void shouldLoadTypeSchemaFromClasspath() {
        // When:
        final ProducerSchema schema = LocalSchemaLoader.loadFromClasspath(TestModel.class);

        // Then:
        assertThat(schema.toString(), containsString("$id: test_model.yml"));
    }

    @Test
    public void shouldThrowIfSchemaNoFound() {
        // When:
        final Exception e =
                assertThrows(
                        LocalSchemaLoader.SchemaResourceNotFoundException.class,
                        () -> LocalSchemaLoader.loadFromClasspath(LocalSchemaLoaderTest.class));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to load schema resource:"
                            + " org/creekservice/internal/kafka/serde/json/schema/LocalSchemaLoaderTest.yml."
                            + " Resource not found."));
    }

    @Test
    public void shouldLoadSchemaFromWithJar() throws Exception {
        // Given:
        final Path jarPath =
                TestPaths.moduleRoot("json-serde").resolve("src/test/resources/schema.jar");

        final String schemaPath = "/schema/test-schema.yml";

        // When:
        final ProducerSchema schema =
                LocalSchemaLoader.load(new URL("jar:file:" + jarPath + "!" + schemaPath));

        // Then:
        assertThat(schema.toString(), containsString("$id: test-schema.yml"));
    }

    @Test
    void shouldBeAbleToLoadSchemaForAnotherModule() {
        // Given:
        final ProducerSchema schema = LocalSchemaLoader.loadFromClasspath(OutputValue.class);

        // Then:
        assertThat(schema.toString(), containsString("title: Output Value"));
    }

    private static final class TestModel {}
}
