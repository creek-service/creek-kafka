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


import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.CreekTestExtension;
import org.creekservice.api.system.test.extension.test.model.TestModelContainer;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicExpectationHandler;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicInputHandler;
import org.creekservice.internal.kafka.streams.test.extension.model.TestOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicInput;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StartKafkaTestListener;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.TearDownTestListener;

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

        api.tests()
                .env()
                .listeners()
                .append(new StartKafkaTestListener(api, clusterEndpointsProvider));

        api.tests().env().listeners().append(new TearDownTestListener(clientsExt));

        initializeModel(api.tests().model(), clientsExt);
    }

    @VisibleForTesting
    public static void initializeModel(
            final TestModelContainer model, final ClientsExtension clientsExt) {
        model.addInput(TopicInput.class, new TopicInputHandler(clientsExt))
                .withName(TopicInput.NAME);
        model.addExpectation(TopicExpectation.class, new TopicExpectationHandler(clientsExt))
                .withName(TopicExpectation.NAME);
        model.addOption(TestOptions.class).withName(TestOptions.NAME);
    }
}
