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
import java.util.stream.Collectors;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler.ExpectationOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TestOptions;

public final class TestOptionsAccessor {

    private TestOptionsAccessor() {}

    public static TestOptions get(final ExpectationOptions options) {
        final List<TestOptions> userSupplied = options.get(TestOptions.class);
        switch (userSupplied.size()) {
            case 0:
                return TestOptions.defaults();
            case 1:
                return userSupplied.get(0);
            default:
                final List<URI> locations =
                        userSupplied.stream()
                                .map(TestOptions::location)
                                .collect(Collectors.toList());
                throw new IllegalArgumentException(
                        "Test suite should only define single '"
                                + TestOptions.NAME
                                + "' option. locations: "
                                + locations);
        }
    }
}