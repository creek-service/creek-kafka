/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.test.api.kafka.serde.test;

import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.service.extension.CreekService;

public final class PublicTestSerdeProvider implements KafkaSerdeProvider {

    public static final SerializationFormat FORMAT =
            SerializationFormat.serializationFormat("test-public");

    public PublicTestSerdeProvider() {}

    @Override
    public SerializationFormat format() {
        return FORMAT;
    }

    @Override
    public SerdeFactory initialize(final CreekService api) {
        return new SerdeFactory() {
            @Override
            public <T> Serde<T> createSerde(final KafkaTopicDescriptor.PartDescriptor<T> part) {
                return null;
            }
        };
    }
}
