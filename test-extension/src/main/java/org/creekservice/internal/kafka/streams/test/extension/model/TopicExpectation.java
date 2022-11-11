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
import static org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord.RecordBuilder.buildRecords;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;
import org.creekservice.api.system.test.extension.test.model.Expectation;

/** An expectation of records on a Kafka topic. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TopicExpectation implements Expectation {

    /** Versioned name, as used in the system test YAML files. */
    public static final String NAME = "creek/kafka-topic@1";

    private final List<TopicRecord> records;

    /**
     * @param topicName optional topic name, can be set at record level
     * @param clusterName optional cluster name, can be set at record level
     * @param ignored ignored notes field.
     * @param records expected records.
     */
    @SuppressWarnings("unused") // Invoked by Jackson via reflection
    public TopicExpectation(
            @JsonProperty(value = "topic") final Optional<String> topicName,
            @JsonProperty(value = "cluster") final Optional<String> clusterName,
            @JsonProperty(value = "notes") final Optional<String> ignored,
            @JsonProperty(value = "records") final List<TopicRecord.RecordBuilder> records) {
        this.records =
                List.copyOf(
                        buildRecords(
                                requireNonNull(clusterName, "clusterName"),
                                requireNonNull(topicName, "topicName"),
                                requireNonNull(records, "records")));

        if (records.isEmpty()) {
            throw new IllegalArgumentException("At least one record is required");
        }
    }

    /**
     * @return the expected records.
     */
    public List<TopicRecord> records() {
        return List.copyOf(records);
    }
}
