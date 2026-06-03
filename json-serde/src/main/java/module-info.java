/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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
import org.creekservice.api.kafka.serde.provider.KafkaSerdeTestExtensionInitializer;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider;
import org.creekservice.internal.kafka.serde.json.JsonSchemaSerdeProvider;
import org.creekservice.internal.kafka.serde.json.JsonSchemaSystemTestSerdeProvider;
import org.creekservice.internal.kafka.serde.json.JsonSchemaTestExtensionInitializer;

/** Provider of JSON serde functionality to Creel. */
module creek.kafka.serde.json.schema {
    requires transitive creek.kafka.serde;
    requires creek.base.type;
    requires creek.observability.logging;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.dataformat.yaml;
    requires com.fasterxml.jackson.datatype.jdk8;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires creek.json.schema.validator;
    requires kafka.schema.registry.client;
    requires kafka.json.schema.provider;
    requires kotlin.stdlib;
    requires org.json;
    requires com.github.luben.zstd_jni;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.annotation;

    provides KafkaSerdeProvider with
            JsonSchemaSerdeProvider;
    provides KafkaSystemTestSerdeProvider with
            JsonSchemaSystemTestSerdeProvider;
    provides KafkaSerdeTestExtensionInitializer with
            JsonSchemaTestExtensionInitializer;

    exports org.creekservice.api.kafka.serde.json;
    exports org.creekservice.api.kafka.serde.json.schema.store.client;
    exports org.creekservice.api.kafka.serde.json.schema;
}
