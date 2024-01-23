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

package org.creekservice.internal.kafka.serde.json.mapper;

import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenericMapperFactoryTest {

    @Mock private JsonMapper jsonMapper;

    @Test
    void shouldRegisterNamedSubtypes() {
        // When:
        new GenericMapperFactory(Map.of(String.class, "bob"), jsonMapper);

        // Then:
        verify(jsonMapper).registerSubtypes(new NamedType(String.class, "bob"));
    }

    @Test
    void shouldRegisterUnnamedSubtypes() {
        // When:
        new GenericMapperFactory(Map.of(String.class, ""), jsonMapper);

        // Then:
        verify(jsonMapper).registerSubtypes(new NamedType(String.class, null));
    }
}