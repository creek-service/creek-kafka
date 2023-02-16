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

/** A model type used to hold the data about a record on a Kafka topic. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TopicRecord {

    private final URI location;
    private final String topicName;
    private final String clusterName;
    private final Optional3<Object> key;
    private final Optional3<Object> value;

    /**
     * @param location the location in the test files the record is persisted.
     * @param clusterName the name of the Kafka cluster.
     * @param topicName the name of the Kafka topic.
     * @param key the record key.
     * @param value the record value.
     */
    @VisibleForTesting
    public TopicRecord(
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

    /**
     * @return the location in the test files the record is persisted.
     */
    public URI location() {
        return location;
    }

    /**
     * @return the name of the Kafka topic.
     */
    public String topicName() {
        return topicName;
    }

    /**
     * @return the name of the Kafka cluster.
     */
    public String clusterName() {
        return clusterName;
    }

    /**
     * @return the record key.
     */
    public Optional3<Object> key() {
        return key;
    }

    /**
     * @return the record value.
     */
    public Optional3<Object> value() {
        return value;
    }

    /**
     * Transform the record to one containing the supplied (coerced) {@code newKey} and {@code
     * newValue}
     *
     * @param newKey the key to set on the new instance.
     * @param newValue the value to set on the new instance.
     * @return a new instance.
     */
    public TopicRecord with(final Optional3<Object> newKey, final Optional3<Object> newValue) {
        return new TopicRecord(location, clusterName, topicName, newKey, newValue);
    }

    @Override
    public String toString() {
        return "TopicRecord{"
                + "clusterName='"
                + clusterName
                + '\''
                + ", topicName='"
                + topicName
                + '\''
                + ", key="
                + key.orElse("<null>", "<any>")
                + ", value="
                + value.orElse("<null>", "<any>")
                + ", location="
                + location
                + '}';
    }

    /** Builder of {@link TopicRecord}. */
    @JsonDeserialize(using = TopicRecord.TopicRecordDeserializer.class)
    public static class RecordBuilder {

        private static final String TOPIC_NOT_SET_ERROR =
                "Topic not set. Topic must be supplied either at the file or record level."
                        + " location: ";

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

        /**
         * Build the {@link TopicRecord} using the supplied defaults.
         *
         * @param defaultCluster the default cluster
         * @param defaultTopic the default topic
         * @return the record.
         */
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

        /**
         * Helper method to build a collection of records from their builders.
         *
         * @param defaultCluster the default cluster
         * @param defaultTopic the default topic
         * @param builders the builders to build from.
         * @return the list of built records.
         */
        public static List<TopicRecord> buildRecords(
                final Optional<String> defaultCluster,
                final Optional<String> defaultTopic,
                final Collection<RecordBuilder> builders) {

            return builders.stream()
                    .map(b -> b.build(defaultCluster, defaultTopic))
                    .collect(Collectors.toUnmodifiableList());
        }
    }

    /** Jackson deserializer for {@link TopicRecord} */
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
                    case "notes":
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
