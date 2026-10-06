/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.creekservice.internal.kafka.test.extension.json;

import static org.creekservice.api.test.hamcrest.PathMatchers.regularFile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
import java.util.Optional;
import org.creekservice.api.system.test.executor.ExecutorOptions;
import org.creekservice.api.system.test.executor.SystemTestExecutor;
import org.creekservice.api.system.test.extension.test.model.TestCaseResult;
import org.creekservice.api.system.test.extension.test.model.TestExecutionResult;
import org.creekservice.api.system.test.extension.test.model.TestSuiteResult;
import org.creekservice.api.test.util.TestPaths;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Run separately from native fixtures: the executor prepares every discovered component. */
@Tag("ContainerisedTest")
class KafkaJsonFunctionalTest {
    @TempDir private Path resultsPath;

    @Test
    void shouldRoundTripJsonSchemaRecords() {
        final Path tests =
                TestPaths.moduleRoot("test-extension")
                        .resolve("src/test/resources/testcases/json_passing");
        final TestExecutionResult result =
                SystemTestExecutor.run(
                        new ExecutorOptions() {
                            @Override
                            public Path testDirectory() {
                                return tests;
                            }

                            @Override
                            public Path resultDirectory() {
                                return resultsPath;
                            }
                        });
        assertThat(result.toString(), result.passed(), is(true));
        assertThat(result.toString(), result.results(), hasSize(1));
        final TestSuiteResult suite = result.results().get(0);
        assertThat(suite.testSuite().name(), is("json schema round trip"));
        assertThat(suite.toString(), suite.error(), is(Optional.empty()));
        assertThat(resultsPath.resolve("TEST-json_schema_round_trip.xml"), is(regularFile()));
        assertThat(suite.toString(), suite.testResults(), hasSize(1));
        final TestCaseResult test = suite.testResults().get(0);
        assertThat(test.testCase().name(), is("schema backed input and output"));
        assertThat(test.skipped(), is(false));
        assertThat(test.toString(), test.failure(), is(Optional.empty()));
        assertThat(test.toString(), test.error(), is(Optional.empty()));
    }
}
