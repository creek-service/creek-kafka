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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.creekservice.internal.kafka.streams.test.extension.yaml.JsonLocation;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TopicRecord {

    private final URI location;
    private final String topicName;
    private final String clusterName;
    private final Optional3<Object> key;
    private final Optional3<Object> value;

    @VisibleForTesting
    TopicRecord(
            final URI location,
            final String clusterName,
            final String topicName,
            final Optional3<Object> key,
            final Optional3<Object> value) {
        this.location = requireNonNull(location, "location");
        this.topicName = requireNonNull(topicName, "topicName");
        this.clusterName = requireNonNull(clusterName, "clusterName");
        this.key = requireNonNull(key, "key");
        this.value = requireNonNull(value, "value");
    }

    public URI location() {
        return location;
    }

    public String topicName() {
        return topicName;
    }

    public String clusterName() {
        return clusterName;
    }

    public Optional3<Object> key() {
        return key;
    }

    public Optional3<Object> value() {
        return value;
    }

    @JsonDeserialize(using = TopicRecord.TopicRecordDeserializer.class)
    public static class RecordBuilder {

        private static final String TOPIC_NOT_SET_ERROR =
                "Topic not set. Topic must be supplied either at the file or record level. location: ";

        final URI location;
        final Optional<String> topicName;
        final Optional<String> clusterName;
        final Optional3<Object> key;
        final Optional3<Object> value;

        @VisibleForTesting
        RecordBuilder(
                final URI location,
                final Optional<String> clusterName,
                final Optional<String> topicName,
                final Optional3<Object> key,
                final Optional3<Object> value) {
            this.location = requireNonNull(location, "location");
            this.topicName = requireNonNull(topicName, "topicName");
            this.clusterName = requireNonNull(clusterName, "clusterName");
            this.key = requireNonNull(key, "key");
            this.value = requireNonNull(value, "value");
        }

        public TopicRecord build(
                final Optional<String> defaultCluster, final Optional<String> defaultTopic) {
            return new TopicRecord(
                    location,
                    clusterName.orElse(
                            defaultCluster.orElse(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME)),
                    topicName.orElseGet(
                            () ->
                                    defaultTopic.orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            TOPIC_NOT_SET_ERROR + location))),
                    key,
                    value);
        }

        public static List<TopicRecord> buildRecords(
                final Optional<String> defaultCluster,
                final Optional<String> defaultTopic,
                final Collection<RecordBuilder> builders) {

            return builders.stream()
                    .map(b -> b.build(defaultCluster, defaultTopic))
                    .collect(Collectors.toUnmodifiableList());
        }
    }

    public static final class TopicRecordDeserializer extends JsonDeserializer<RecordBuilder> {
        @Override
        public RecordBuilder deserialize(final JsonParser parser, final DeserializationContext ctx)
                throws IOException {

            final URI location = JsonLocation.location(parser);
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
                    case "note":
                        parser.nextToken();
                        break;
                    default:
                        throw new JsonParseException(parser, "Unknown property: " + fieldName);
                }
            }

            return new RecordBuilder(location, cluster, topic, key, value);
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
