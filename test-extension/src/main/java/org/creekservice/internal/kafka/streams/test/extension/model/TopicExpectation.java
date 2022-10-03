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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import org.creekservice.api.system.test.extension.test.model.Expectation;
import org.creekservice.api.system.test.extension.test.model.LocationAware;

public final class TopicExpectation implements Expectation, LocationAware<TopicExpectation> {

    private final URI location;

    public TopicExpectation(@JsonProperty("dummy") final String dummy) {
        this(LocationAware.UNKNOWN_LOCATION);
    }

    private TopicExpectation(final URI location) {
        this.location = requireNonNull(location, "location");
    }

    @Override
    public URI location() {
        return location;
    }

    @Override
    public TopicExpectation withLocation(final URI location) {
        return new TopicExpectation(location);
    }
}
