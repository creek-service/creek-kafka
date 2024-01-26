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

import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import java.util.Objects;
import java.util.Optional;
import org.creekservice.internal.kafka.serde.json.schema.YamlSchemas;

/**
 * A consumer schema uses an open-content model
 *
 * @see <a
 *     href="https://www.creekservice.org/articles/2024/01/09/json-schema-evolution-part-2.html">Article
 *     on JSON schema evolution</a>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class ConsumerSchema implements YamlSchema {

    private final String yaml;
    private transient Optional<String> json = Optional.empty();

    ConsumerSchema(final String yaml) {
        this.yaml = requireNonBlank(yaml, "yaml");
    }

    @Override
    public String asJsonText() {
        if (json.isEmpty()) {
            json = Optional.of(YamlSchemas.yamlToJson(yaml));
        }
        return json.get();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConsumerSchema that = (ConsumerSchema) o;
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
}
