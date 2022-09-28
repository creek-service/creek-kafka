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

package org.creekservice.internal.kafka.streams.test.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
import org.creekservice.api.system.test.executor.ExecutorOptions;
import org.creekservice.api.system.test.executor.SystemTestExecutor;
import org.creekservice.api.test.util.TestPaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaTestExtensionFunctionalTest {

    private static final Path TESTCASES =
            TestPaths.moduleRoot("test-extension").resolve("src/test/resources/testcases");

    @TempDir private Path resultsDir;

    @Test
    void shouldRunTests() {
        final ExecutorOptions options = executorOptions("passing");

        // When:
        final boolean allPassed = SystemTestExecutor.run(options);

        // Then:
        assertThat(allPassed, is(true));
    }

    private ExecutorOptions executorOptions(final String suite) {
        return new ExecutorOptions() {
            @Override
            public Path testDirectory() {
                return TESTCASES.resolve(suite);
            }

            @Override
            public Path resultDirectory() {
                return resultsDir;
            }
        };
    }
}
