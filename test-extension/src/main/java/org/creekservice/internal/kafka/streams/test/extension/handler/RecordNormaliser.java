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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import java.util.Collection;
import java.util.List;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;

final class RecordNormaliser {

    List<TopicRecord> normalise(
            final Collection<TopicRecord> records, final TestKafkaTopic testTopic) {
        return records.stream().map(r -> normalise(r, testTopic)).toList();
    }

    private TopicRecord normalise(final TopicRecord record, final TestKafkaTopic testTopic) {
        boolean handlingKey = true;
        try {
            final Optional3<Object> key = record.key().map(testTopic::normaliseKey);
            handlingKey = false;
            final Optional3<Object> value = record.value().map(testTopic::normaliseValue);
            return record.with(key, value);
        } catch (final Exception e) {
            final String fieldName = handlingKey ? "key" : "value";
            final Object data =
                    handlingKey ? record.key().orElseThrow() : record.value().orElseThrow();
            final Class<?> type =
                    handlingKey
                            ? testTopic.descriptor().key().type()
                            : testTopic.descriptor().value().type();
            throw new TopicExpectationException(
                    "The record's "
                            + fieldName
                            + " is not compatible with the topic's "
                            + fieldName
                            + " type."
                            + " "
                            + fieldName
                            + ": "
                            + data
                            + ", "
                            + fieldName
                            + "_type: "
                            + data.getClass().getName()
                            + ", topic_"
                            + fieldName
                            + "_type: "
                            + type.getName(),
                    e);
        }
    }
}
