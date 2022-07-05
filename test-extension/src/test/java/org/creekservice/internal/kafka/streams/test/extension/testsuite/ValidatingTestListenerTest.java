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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.service.ConfigurableServiceInstance;
import org.creekservice.internal.kafka.common.resource.KafkaResourceValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class ValidatingTestListenerTest {

    @Mock(answer = RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    @Mock private ConfigurableServiceInstance inst0;
    @Mock private ConfigurableServiceInstance inst1;
    @Mock private ServiceDescriptor descriptor0;
    @Mock private ServiceDescriptor descriptor1;
    @Mock private KafkaResourceValidator validator;
    private final List<ComponentDescriptor> validatedComponents = new ArrayList<>();
    private ValidatingTestListener listener;

    @BeforeEach
    void setUp() {
        validatedComponents.clear();

        listener = new ValidatingTestListener(api, validator);

        when(api.testSuite().services().stream()).thenReturn(Stream.of(inst0, inst1));
        when(inst0.descriptor()).thenReturn(Optional.of(descriptor0));
        when(inst1.descriptor()).thenReturn(Optional.of(descriptor1));
        doAnswer(trackValidatedComponents()).when(validator).validate(any());
    }

    @Test
    void shouldValidateServiceDescriptors() {
        // When:
        listener.beforeSuite(null);

        // Then:
        assertThat(validatedComponents, contains(descriptor0, descriptor1));
    }

    @Test
    void shouldIgnoreServicesWithOutDescriptors() {
        // Given:
        when(inst0.descriptor()).thenReturn(Optional.empty());

        // When:
        listener.beforeSuite(null);

        // Then:
        assertThat(validatedComponents, contains(descriptor1));
    }

    @Test
    void shouldThrowOnInvalidDescriptor() {
        // Given:
        final IllegalArgumentException cause = new IllegalArgumentException("BOOM");
        doThrow(cause).when(validator).validate(any());

        // When:
        final Exception e =
                assertThrows(IllegalArgumentException.class, () -> listener.beforeSuite(null));

        // Then:
        assertThat(e, is(sameInstance(cause)));
    }

    private Answer<Void> trackValidatedComponents() {
        return inv -> {
            final Stream<? extends ComponentDescriptor> s = inv.getArgument(0);
            s.forEachOrdered(validatedComponents::add);
            return null;
        };
    }
}
