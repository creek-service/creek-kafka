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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.system.test.extension.test.model.Input;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TopicInput implements Input {

    private static final String TOPIC_NOT_SET_ERROR =
            "Topic not set. " + "Topic must be supplied either at the file or record level.";

    private final Optional<String> topicName;
    private final Optional<String> clusterName;
    private final Optional<String> notes;
    private final List<TopicRecord> records;

    public TopicInput(
            @JsonProperty(value = "topic") final Optional<String> topicName,
            @JsonProperty(value = "cluster") final Optional<String> clusterName,
            @JsonProperty(value = "notes") final Optional<String> notes,
            @JsonProperty(value = "records") final List<TopicRecord> records) {
        this.topicName = requireNonNull(topicName, "topicName");
        this.clusterName = requireNonNull(clusterName, "clusterName");
        this.notes = requireNonNull(notes, "notes");
        this.records = build(topicName, clusterName, requireNonNull(records, "records"));
    }

    @JsonGetter("topic")
    public Optional<String> topicName() {
        return topicName;
    }

    @JsonGetter("cluster")
    public Optional<String> clusterName() {
        return clusterName;
    }

    @JsonGetter("notes")
    public Optional<String> notes() {
        return notes;
    }

    @JsonGetter("records")
    public List<TopicRecord> records() {
        return List.copyOf(records);
    }

    private static List<TopicRecord> build(
            final Optional<String> topicName,
            final Optional<String> clusterName,
            final List<TopicRecord> records) {

        if (records.isEmpty()) {
            throw new IllegalArgumentException("At least one record is required");
        }

        return records.stream()
                .map(
                        r ->
                                r.topicName().isPresent()
                                        ? r
                                        : r.withTopicName(
                                                topicName.orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        TOPIC_NOT_SET_ERROR))))
                .map(
                        r ->
                                r.clusterName().isPresent()
                                        ? r
                                        : r.withClusterName(
                                                clusterName.orElse(
                                                        KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME)))
                .collect(Collectors.toUnmodifiableList());
    }
}
