/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.CreekTestExtension;
import org.creekservice.api.system.test.extension.test.env.listener.TestListenerContainer;
import org.creekservice.api.system.test.extension.test.model.TestModelContainer;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicExpectationHandler;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicInputHandler;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicInput;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StartKafkaTestListener;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.TopicValidatingListener;

/**
 * A Creek system test extension for testing Kafka Streams based microservices.
 *
 * <p>The extension will start any required Kafka clusters and handle any Kafka related inputs and
 * expectations defined in system tests.
 */
public final class KafkaTestExtension implements CreekTestExtension {
    @Override
    public String name() {
        return "org.creekservice.kafka.test";
    }

    @Override
    public void initialize(final CreekSystemTest api) {
        final ClusterEndpointsProvider clusterEndpointsProvider = new ClusterEndpointsProvider();
        api.extensions()
                .addOption(
                        KafkaClientsExtensionOptions.builder()
                                .withKafkaPropertiesOverrides(clusterEndpointsProvider)
                                .build());

        final ClientsExtension clientsExt =
                (ClientsExtension)
                        api.extensions().ensureExtension(KafkaClientsExtensionProvider.class);

        final TestListenerContainer testListeners = api.tests().env().listeners();

        testListeners.append(new StartKafkaTestListener(api, clusterEndpointsProvider));

        final TopicValidatingListener topicValidator = new TopicValidatingListener(api);
        testListeners.append(topicValidator);

        initializeModel(
                api.tests().model(),
                new TopicInputHandler(clientsExt, topicValidator),
                new TopicExpectationHandler(clientsExt, topicValidator));
    }

    /**
     * @param model the test model
     * @param inputHandler the input handler
     * @param expectationHandler the expectation handler.
     */
    @VisibleForTesting
    public static void initializeModel(
            final TestModelContainer model,
            final TopicInputHandler inputHandler,
            final TopicExpectationHandler expectationHandler) {
        model.addInput(TopicInput.class, inputHandler).withName(TopicInput.NAME);
        model.addExpectation(TopicExpectation.class, expectationHandler)
                .withName(TopicExpectation.NAME);
        model.addOption(KafkaOptions.class).withName(KafkaOptions.NAME);
    }
}
