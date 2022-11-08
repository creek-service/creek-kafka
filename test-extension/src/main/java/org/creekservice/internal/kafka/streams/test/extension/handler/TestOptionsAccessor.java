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


import java.net.URI;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler.ExpectationOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;

/** Util class for accessing {@link KafkaOptions}. */
public final class TestOptionsAccessor {

    private TestOptionsAccessor() {}

    /**
     * Get Kafka options from a {@link ExpectationOptions}.
     *
     * @param options the expectation options.
     * @return the Kafka options.
     */
    public static KafkaOptions get(final ExpectationOptions options) {
        return get(options::get);
    }

    /**
     * Get Kafka options from a {@link CreekTestSuite}.
     *
     * @param suite the test suite.
     * @return the Kafka options.
     */
    public static KafkaOptions get(final CreekTestSuite suite) {
        return get(suite::options);
    }

    private static KafkaOptions get(
            final Function<Class<KafkaOptions>, List<KafkaOptions>> provider) {
        final List<KafkaOptions> userSupplied = provider.apply(KafkaOptions.class);
        switch (userSupplied.size()) {
            case 0:
                return KafkaOptions.defaults();
            case 1:
                return userSupplied.get(0);
            default:
                final List<URI> locations =
                        userSupplied.stream()
                                .map(KafkaOptions::location)
                                .collect(Collectors.toList());
                throw new IllegalArgumentException(
                        "Test suite should only define single '"
                                + KafkaOptions.NAME
                                + "' option. locations: "
                                + locations);
        }
    }
}
