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

import static org.creekservice.api.test.hamcrest.PathMatchers.regularFile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.nio.file.Path;
import java.util.Optional;
import org.creekservice.api.system.test.executor.ExecutorOptions;
import org.creekservice.api.system.test.executor.SystemTestExecutor;
import org.creekservice.api.system.test.extension.test.model.TestExecutionResult;
import org.creekservice.api.test.util.TestPaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaTestExtensionFunctionalTest {

    private static final Path TESTCASES =
            TestPaths.moduleRoot("test-extension").resolve("src/test/resources/testcases");

    @TempDir private Path resultsDir;

    @Test
    void shouldDetectSuccess() {
        final ExecutorOptions options = executorOptions("passing");

        // When:
        final TestExecutionResult result = SystemTestExecutor.run(options);

        // Then:
        assertThat(result.toString(), result.passed(), is(true));
        assertThat(resultsDir.resolve("TEST-passing_suite.xml"), is(regularFile()));
    }

    @Test
    void shouldDetectExpectationFailures() {
        final ExecutorOptions options = executorOptions("expectation_failure");

        // When:
        final TestExecutionResult result = SystemTestExecutor.run(options);

        // Then:
        assertThat(result.toString(), result.passed(), is(false));
        assertThat(resultsDir.resolve("TEST-expectation_failures.xml"), is(regularFile()));
        assertThat(failureMessage(result, 0, 0), containsString("Additional records"));
        assertThat(failureMessage(result, 0, 1), containsString("1 expected record(s) not found"));
        assertThat(
                failureMessage(result, 0, 1),
                containsString("(Mismatch@key@char5, expected: Long(-2), actual: Long(2))"));
        assertThat(failureMessage(result, 0, 2), containsString("1 expected record(s) not found"));
        assertThat(
                failureMessage(result, 0, 2),
                containsString("(Mismatch@key@char0, expected: <empty>, actual: Long(2))"));
        assertThat(failureMessage(result, 0, 3), containsString("1 expected record(s) not found"));
        assertThat(
                failureMessage(result, 0, 3),
                containsString(
                        "(Mismatch@value@char7, expected: String(dad), actual: String(mum))"));
        assertThat(failureMessage(result, 0, 4), containsString("1 expected record(s) not found"));
        assertThat(
                failureMessage(result, 0, 4),
                containsString("(Mismatch@value@char0, expected: <empty>, actual: String(mum))"));
        assertThat(failureMessage(result, 1, 0), containsString("1 expected record(s) not found."));
        assertThat(
                failureMessage(result, 1, 0),
                containsString("(Records match, but the order is wrong)"));
    }

    @Test
    void shouldDetectErrors() {
        final ExecutorOptions options = executorOptions("errors");

        // When:
        final TestExecutionResult result = SystemTestExecutor.run(options);

        // Then:
        assertThat(result.toString(), result.passed(), is(false));
        assertThat(resultsDir.resolve("TEST-errors.xml"), is(regularFile()));

        assertThat(
                errorMessage(result, 0),
                containsString(
                        "Test run failed for test case: unknown input topic, cause: "
                                + "The record's cluster or topic is not known. "
                                + "cluster: default, topic: unknown-input"));

        assertThat(
                errorMessage(result, 1),
                containsString(
                        "Test run failed for test case: unknown output topic, cause: "
                                + "The expected record's cluster or topic is not known. "
                                + "cluster: default, topic: unknown-output"));

        assertThat(
                errorMessage(result, 2),
                containsString(
                        "Test run failed for test case: bad input key, cause: "
                                + "The record's key is not compatible with the topic's key type. "
                                + "key: [not a string], key_type: java.util.ArrayList, "
                                + "topic_key_type: java.lang.String, topic: input"));

        assertThat(
                errorMessage(result, 3),
                containsString(
                        "Test run failed for test case: bad input value, cause: "
                                + "The record's value is not compatible with the topic's value type. "
                                + "value: not a number, value_type: java.lang.String, "
                                + "topic_value_type: java.lang.Long, topic: input"));

        assertThat(
                errorMessage(result, 4),
                containsString(
                        "Test run failed for test case: bad output key, cause: "
                                + "Failed to deserialize record key. topic: output, partition: 0, offset: 0"));
    }

    private static String failureMessage(
            final TestExecutionResult result, final int suiteIndex, final int caseIndex) {
        assertThat(result.toString(), result.results(), hasSize(greaterThan(suiteIndex)));
        assertThat(
                result.toString(),
                result.results().get(suiteIndex).testCases(),
                hasSize(greaterThan(caseIndex)));
        assertThat(
                result.toString(),
                result.results().get(suiteIndex).testCases().get(caseIndex).failure(),
                not(Optional.empty()));
        return result.results()
                .get(suiteIndex)
                .testCases()
                .get(caseIndex)
                .failure()
                .map(Throwable::getMessage)
                .orElse("");
    }

    private static String errorMessage(final TestExecutionResult result, final int index) {
        assertThat(result.toString(), result.results(), hasSize(1));
        assertThat(
                result.toString(),
                result.results().get(0).testCases(),
                hasSize(greaterThan(index)));
        assertThat(
                result.toString(),
                result.results().get(0).testCases().get(index).error(),
                not(Optional.empty()));
        return result.results()
                .get(0)
                .testCases()
                .get(index)
                .error()
                .map(Throwable::getMessage)
                .orElse("");
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
