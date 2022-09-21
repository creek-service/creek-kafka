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

package org.creekservice.internal.kafka.streams.test.extension.model;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.Optional;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@JsonDeserialize(using = TopicRecord.TopicRecordDeserializer.class)
public final class TopicRecord {

    private final Optional<String> topicName;
    private final Optional<String> clusterName;
    private final Optional3<Object> key;
    private final Optional3<Object> value;

    @VisibleForTesting
    TopicRecord(
            final Optional<String> topicName,
            final Optional<String> clusterName,
            final Optional3<Object> key,
            final Optional3<Object> value) {
        this.topicName = requireNonNull(topicName, "topicName");
        this.clusterName = requireNonNull(clusterName, "clusterName");
        this.key = requireNonNull(key, "key");
        this.value = requireNonNull(value, "value");
    }

    @JsonGetter("topic")
    public Optional<String> topicName() {
        return topicName;
    }

    @JsonGetter("cluster")
    public Optional<String> clusterName() {
        return clusterName;
    }

    @JsonGetter("key")
    public Optional3<Object> key() {
        return key;
    }

    @JsonGetter("value")
    public Optional3<Object> value() {
        return value;
    }

    public TopicRecord withTopicName(final String topicName) {
        return new TopicRecord(Optional.of(topicName), clusterName, key, value);
    }

    public TopicRecord withClusterName(final String clusterName) {
        return new TopicRecord(topicName, Optional.of(clusterName), key, value);
    }

    public static final class TopicRecordDeserializer extends JsonDeserializer<TopicRecord> {
        @Override
        public TopicRecord deserialize(final JsonParser parser, final DeserializationContext ctx)
                throws IOException {

            Optional<String> topic = Optional.empty();
            Optional<String> cluster = Optional.empty();
            Optional3<Object> key = Optional3.notProvided();
            Optional3<Object> value = Optional3.notProvided();

            String fieldName;
            while ((fieldName = parser.nextFieldName()) != null) {
                switch (fieldName) {
                    case "topic":
                        topic = Optional.ofNullable(parser.nextTextValue());
                        requireNonBlank(topic, "topic", parser);
                        break;
                    case "cluster":
                        cluster = Optional.ofNullable(parser.nextTextValue());
                        requireNonBlank(cluster, "cluster", parser);
                        break;
                    case "key":
                        parser.nextToken();
                        key = Optional3.ofNullable(parser.readValueAs(Object.class));
                        break;
                    case "value":
                        parser.nextToken();
                        value = Optional3.ofNullable(parser.readValueAs(Object.class));
                        break;
                    default:
                        throw new JsonParseException(parser, "Unknown property: " + fieldName);
                }
            }

            return new TopicRecord(topic, cluster, key, value);
        }

        private static void requireNonBlank(
                final Optional<String> val, final String name, final JsonParser parser)
                throws IOException {
            if (val.isPresent() && val.get().isBlank()) {
                throw new JsonParseException(parser, "Property can not be blank: " + name);
            }
        }
    }
}
