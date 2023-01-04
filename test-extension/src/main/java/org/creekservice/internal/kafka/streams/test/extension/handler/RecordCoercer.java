/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.creekservice.internal.kafka.streams.test.extension.yaml.TypeCoercer;

final class RecordCoercer {

    private final TypeCoercer typeCoercer = new TypeCoercer();

    List<TopicRecord> coerce(
            final Collection<TopicRecord> records, final KafkaTopicDescriptor<?, ?> topic) {
        return records.stream().map(r -> coerce(r, topic)).collect(Collectors.toList());
    }

    private TopicRecord coerce(final TopicRecord record, final KafkaTopicDescriptor<?, ?> topic) {
        try {
            final Optional3<Object> key =
                    record.key().map(k -> coerce("key", k, topic.key().type()));

            final Optional3<Object> value =
                    record.value().map(v -> coerce("value", v, topic.value().type()));

            return record.with(key, value);
        } catch (final Exception e) {
            throw new TopicExpectationException(
                    "Failed to coerce expected record."
                            + " topic: "
                            + topic.name()
                            + ", location: "
                            + record.location(),
                    e);
        }
    }

    private <T> T coerce(final String fieldName, final Object value, final Class<T> type) {
        try {
            return typeCoercer.coerce(value, type);
        } catch (final Exception e) {
            throw new TopicExpectationException(
                    "The record's "
                            + fieldName
                            + " is not compatible with the topic's "
                            + fieldName
                            + " type."
                            + " "
                            + fieldName
                            + ": "
                            + value
                            + ", "
                            + fieldName
                            + "_type: "
                            + value.getClass().getName()
                            + ", topic_"
                            + fieldName
                            + "_type: "
                            + type.getName(),
                    e);
        }
    }
}
