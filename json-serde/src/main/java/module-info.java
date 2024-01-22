/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.internal.kafka.serde.json.JsonSchemaSerdeProvider;

/** Provider of JSON serde functionality to Creel. */
module creek.kafka.serde.json.schema {
    requires transitive creek.kafka.serde;
    requires creek.base.type;
    requires creek.observability.logging;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.dataformat.yaml;
    requires com.fasterxml.jackson.datatype.jdk8;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires jimblackler.jsonschemafriend.core;
    requires kafka.schema.registry.client;
    requires kafka.json.schema.provider;

    provides KafkaSerdeProvider with
            JsonSchemaSerdeProvider;

    exports org.creekservice.api.kafka.serde.json.schema.store.endpoint;
}
