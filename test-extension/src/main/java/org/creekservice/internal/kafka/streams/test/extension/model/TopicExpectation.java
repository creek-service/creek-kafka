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
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.creekservice.api.system.test.extension.test.model.Expectation;
import org.creekservice.api.system.test.extension.test.model.LocationAware;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TopicExpectation implements Expectation, LocationAware<TopicExpectation> {

    private final URI location;
    private final List<TopicRecord> records;

    @SuppressWarnings("unused") // Invoked by Jackson via reflection
    public TopicExpectation(
            @JsonProperty(value = "topic") final Optional<String> topicName,
            @JsonProperty(value = "cluster") final Optional<String> clusterName,
            @JsonProperty(value = "notes") final Optional<String> ignored,
            @JsonProperty(value = "records") final List<TopicRecord.RecordBuilder> records) {
        this(
                buildRecords(
                        requireNonNull(clusterName, "clusterName"),
                        requireNonNull(topicName, "topicName"),
                        requireNonNull(records, "records")),
                LocationAware.UNKNOWN_LOCATION);
    }

    public TopicExpectation(final List<TopicRecord> records, final URI location) {
        this.location = requireNonNull(location, "location");
        this.records = List.copyOf(requireNonNull(records, "records"));
        if (records.isEmpty()) {
            throw new IllegalArgumentException("At least one record is required");
        }
    }

    public List<TopicRecord> records() {
        return List.copyOf(records);
    }

    @Override
    public URI location() {
        return location;
    }

    @Override
    public TopicExpectation withLocation(final URI location) {
        return new TopicExpectation(records, location);
    }
}
