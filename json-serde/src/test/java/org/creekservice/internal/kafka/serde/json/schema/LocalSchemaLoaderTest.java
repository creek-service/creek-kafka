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

package org.creekservice.internal.kafka.serde.json.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.creekservice.api.test.util.TestPaths;
import org.junit.jupiter.api.Test;

class LocalSchemaLoaderTest {

    @Test
    public void shouldLoadTypeSchemaFromClasspath() {
        // When:
        final JsonSchema schema = LocalSchemaLoader.loadFromClasspath(TestModel.class);

        // Then:
        assertThat(schema.getString("$id"), is("test_model.yml"));
    }

    @Test
    public void shouldLoadPathSchemaFromClasspath() {
        // When:
        final JsonSchema schema = LocalSchemaLoader.loadFromClasspath(Paths.get("test-schema.yml"));

        // Then:
        assertThat(schema.getString("$id"), is("test-schema.yml"));
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    @Test
    public void shouldThrowIfSchemaNoFound() {
        // When:
        final Exception e =
                assertThrows(
                        LocalSchemaLoader.SchemaResourceNotFoundException.class,
                        () -> LocalSchemaLoader.loadFromClasspath(Paths.get("u_wont_find_me.yml")));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to load schema resource: /schema/json/u_wont_find_me.yml."
                                + " Resource not found."));
    }

    @Test
    public void shouldThrowIfSchemaNotValidYaml() throws Exception {
        // When:
        final Exception e =
                assertThrows(
                        LocalSchemaLoader.InvalidSchemaException.class,
                        () ->
                                LocalSchemaLoader.loadFromClasspath(
                                        Paths.get("invalid-yaml-schema.yml")));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to convert schema to JSON: "
                                + Paths.get(
                                                "build",
                                                "resources",
                                                "test",
                                                "schema",
                                                "json",
                                                "invalid-yaml-schema.yml")
                                        .toAbsolutePath()
                                        .toUri()
                                        .toURL()
                                + "."));
        assertThat(e.getCause(), is(instanceOf(JacksonYAMLParseException.class)));
    }

    @Test
    public void shouldThrowIfSchemaInvalid() throws Exception {
        // When:
        final Exception e =
                assertThrows(
                        LocalSchemaLoader.InvalidSchemaException.class,
                        () -> LocalSchemaLoader.loadFromClasspath(Paths.get("invalid-schema.yml")));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Schema was invalid: "
                                + Paths.get(
                                                "build",
                                                "resources",
                                                "test",
                                                "schema",
                                                "json",
                                                "invalid-schema.yml")
                                        .toAbsolutePath()
                                        .toUri()
                                        .toURL()
                                + "."));
    }

    @Test
    public void shouldLoadSchemaFromWithJar() throws Exception {
        // Given:
        final Path jarPath =
                TestPaths.moduleRoot("json-serde").resolve("src/test/resources/schema.jar");

        final String schemaPath = "/schema/test-schema.yml";

        // When:
        final JsonSchema schema =
                LocalSchemaLoader.load(new URL("jar:file:" + jarPath + "!" + schemaPath));

        // Then:
        assertThat(schema.getString("$id"), is("test-schema.yml"));
    }

    private static final class TestModel {}
}
