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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import org.creekservice.api.system.test.extension.test.env.listener.TestEnvironmentListener;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.test.model.TestSuiteResult;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TearDownTestListener implements TestEnvironmentListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownTestListener.class);

    private final ClientsExtension clientsExt;

    public TearDownTestListener(final ClientsExtension clientsExt) {
        this.clientsExt = requireNonNull(clientsExt, "clientsExt");
    }

    @Override
    public void afterSuite(final CreekTestSuite suite, final TestSuiteResult result) {
        try {
            clientsExt.close(Duration.ofMinutes(1));
        } catch (final Exception e) {
            LOGGER.error("Failed to close Kafka clients used during the system test suite.", e);
        }
    }
}
