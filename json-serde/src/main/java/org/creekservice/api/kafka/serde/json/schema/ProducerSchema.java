/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.serde.json.schema;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import java.util.Objects;
import java.util.Optional;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;
import org.creekservice.internal.kafka.serde.json.schema.YamlSchemas;

/**
 * A producer schema uses a closed-content model
 *
 * @see <a
 *     href="https://www.creekservice.org/articles/2024/01/09/json-schema-evolution-part-2.html">Article
 *     on JSON schema evolution</a>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class ProducerSchema implements YamlSchema {

    private final String yaml;
    private transient Optional<String> json;

    private ProducerSchema(final String yaml, final Optional<String> json) {
        this.yaml = requireNonBlank(yaml, "yaml");
        this.json = requireNonNull(json, "json");
    }

    public static ProducerSchema fromYaml(final String yaml) {
        final ProducerSchema schema = new ProducerSchema(yaml, Optional.empty());
        schema.validateYaml();
        schema.validateClosedContentModel();
        return schema;
    }

    public static ProducerSchema fromJson(final String json) {
        final ProducerSchema schema =
                new ProducerSchema(YamlSchemas.jsonToYaml(json), Optional.of(json));
        schema.validateClosedContentModel();
        return schema;
    }

    @Override
    public String asJsonText() {
        if (json.isEmpty()) {
            json = Optional.of(YamlSchemas.yamlToJson(yaml));
        }
        return json.get();
    }

    /**
     * @return the schema with all objects converted to an open-content model.
     */
    public ConsumerSchema toConsumerSchema() {
        return new ConsumerSchema(YamlSchemas.toOpenContentModel(yaml));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ProducerSchema that = (ProducerSchema) o;
        return Objects.equals(yaml, that.yaml);
    }

    @Override
    public int hashCode() {
        return Objects.hash(yaml);
    }

    @Override
    public String toString() {
        return yaml;
    }

    private void validateYaml() {
        YamlSchemas.validate(yaml);
    }

    private void validateClosedContentModel() {
        if (!YamlSchemas.isClosedContentModel(yaml)) {
            throw new OpenSchemaException(yaml);
        }
    }

    @VisibleForTesting
    static final class OpenSchemaException extends SchemaException {
        OpenSchemaException(final String yaml) {
            super("Producer schemas must have a closed content model: " + yaml);
        }
    }
}
