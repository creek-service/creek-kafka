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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
final class ConsumedRecord {

    private final ConsumerRecord<byte[], byte[]> record;
    private final Optional<?> deserializedKey;
    private final Optional<?> deserializedValue;

    ConsumedRecord(
            final ConsumerRecord<byte[], byte[]> record,
            final Optional<?> deserializedKey,
            final Optional<?> deserializedValue) {
        this.record = requireNonNull(record, "record");
        this.deserializedKey = requireNonNull(deserializedKey, "deserializedKey");
        this.deserializedValue = requireNonNull(deserializedValue, "deserializedValue");
    }

    Optional<?> key() {
        return deserializedKey;
    }

    Optional<?> value() {
        return deserializedValue;
    }

    @Override
    public String toString() {
        return "ConsumedRecord{"
                + "record="
                + record
                + ", deserializedKey="
                + deserializedKey.map(Objects::toString).orElse("<null>")
                + ", deserializedValue="
                + deserializedValue.map(Objects::toString).orElse("<null>")
                + '}';
    }
}
