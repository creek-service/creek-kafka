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

import static java.lang.System.lineSeparator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import org.junit.jupiter.api.Test;

class ConsumerSchemaTest {

    private static final String OPEN_YAML =
            "---\n"
                    + "\"$schema\": http://json-schema.org/draft-07/schema#\n"
                    + "type: object\n"
                    + "additionalProperties: true\n"
                    + "properties:\n"
                    + "  foo:\n"
                    + "    type: integer\n"
                    + "  bar:\n"
                    + "    type: string\n"
                    + "    format: date\n";

    private static final String OPEN_JSON =
            "{\n"
                    + "  \"$schema\" : \"http://json-schema.org/draft-07/schema#\",\n"
                    + "  \"type\" : \"object\",\n"
                    + "  \"additionalProperties\" : true,\n"
                    + "  \"properties\" : {\n"
                    + "    \"foo\" : {\n"
                    + "      \"type\" : \"integer\"\n"
                    + "    },\n"
                    + "    \"bar\" : {\n"
                    + "      \"type\" : \"string\",\n"
                    + "      \"format\" : \"date\"\n"
                    + "    }\n"
                    + "  }\n"
                    + "}";

    @Test
    void shouldThrowNPEs() {
        new NullPointerTester().testAllPublicStaticMethods(ConsumerSchema.class);
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        new ConsumerSchema("----\ntrue"), new ConsumerSchema("----\ntrue"))
                .addEqualityGroup(new ConsumerSchema("----\nfalse"))
                .addEqualityGroup(new ConsumerSchema(OPEN_YAML))
                .testEquals();
    }

    @Test
    void shouldConvertToJson() {
        // Given:
        final ConsumerSchema schema = new ConsumerSchema(OPEN_YAML);

        // When:
        final String result = schema.asJsonText();

        // Then:
        assertThat(result, is(OPEN_JSON.replaceAll("\n", lineSeparator())));
    }
}
