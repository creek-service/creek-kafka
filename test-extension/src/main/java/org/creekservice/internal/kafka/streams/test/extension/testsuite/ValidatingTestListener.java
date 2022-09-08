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

import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.test.env.listener.TestEnvironmentListener;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.internal.kafka.common.resource.KafkaResourceValidator;

public final class ValidatingTestListener implements TestEnvironmentListener {

    private final CreekSystemTest api;
    private final KafkaResourceValidator validator;

    public ValidatingTestListener(final CreekSystemTest api) {
        this(api, new KafkaResourceValidator());
    }

    @VisibleForTesting
    ValidatingTestListener(final CreekSystemTest api, final KafkaResourceValidator validator) {
        this.api = requireNonNull(api, "api");
        this.validator = requireNonNull(validator, "validator");
    }

    @Override
    public void beforeSuite(final CreekTestSuite suite) {
        final Stream<ServiceDescriptor> descriptors =
                api.test().env().currentSuite().services().stream()
                        .map(ServiceInstance::descriptor)
                        .flatMap(Optional::stream);

        validator.validate(descriptors);
    }
}
