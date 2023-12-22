/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.test.internal.kafka.serde.test;

import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;

public final class PrivateTestSerdeProvider implements KafkaSerdeProvider {

    public static final SerializationFormat FORMAT =
            SerializationFormat.serializationFormat("test-private");

    @Override
    public SerializationFormat format() {
        return FORMAT;
    }

    @Override
    public SerdeProvider initialize(final String clusterName, final InitializeParams params) {
        return new SerdeProvider() {
            @Override
            public <T> Serde<T> createSerde(final KafkaTopicDescriptor.PartDescriptor<T> part) {
                return null;
            }
        };
    }
}
