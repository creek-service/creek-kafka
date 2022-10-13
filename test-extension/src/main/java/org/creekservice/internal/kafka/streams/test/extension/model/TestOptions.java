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
import java.util.Optional;
import org.creekservice.api.system.test.extension.test.model.LocationAware;
import org.creekservice.api.system.test.extension.test.model.Option;

/** Test model extension to allow users to customise how this test extension operates. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class TestOptions implements Option, LocationAware<TestOptions> {

    public static final String NAME = "creek/kafka-options@1";

    private static final TestOptions DEFAULTS = new TestOptions(Optional.empty());

    public enum OutputOrdering {
        /** Topic records can be in any order. */
        NONE,

        /**
         * Records sharing the same key must be received in the same order they are defined in the
         * expectations.
         */
        BY_KEY
    }

    private final URI location;
    private final OutputOrdering outputOrdering;

    public static TestOptions defaults() {
        return DEFAULTS;
    }

    @SuppressWarnings("unused") // Invoked by Jackson via reflection
    public TestOptions(
            @JsonProperty("outputOrdering") final Optional<OutputOrdering> outputOrdering) {
        this(outputOrdering.orElse(OutputOrdering.BY_KEY), LocationAware.UNKNOWN_LOCATION);
    }

    private TestOptions(final OutputOrdering outputOrdering, final URI location) {
        this.outputOrdering = requireNonNull(outputOrdering, "outputOrdering");
        this.location = requireNonNull(location, "location");
    }

    public OutputOrdering outputOrdering() {
        return outputOrdering;
    }

    @Override
    public TestOptions withLocation(final URI location) {
        return new TestOptions(outputOrdering, location);
    }

    @Override
    public URI location() {
        return location;
    }
}
