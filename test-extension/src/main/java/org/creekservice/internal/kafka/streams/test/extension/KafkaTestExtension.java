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


import java.util.Collection;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.CreekTestExtension;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler;
import org.creekservice.api.system.test.extension.test.model.TestModelContainer;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicInputHandler;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicInput;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StartKafkaTestListener;

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
        api.extensions().ensureExtension(KafkaClientsExtensionProvider.class);
        api.tests()
                .env()
                .listeners()
                .append(new StartKafkaTestListener(api, clusterEndpointsProvider));

        initializeModel(api.tests().model());
    }

    @VisibleForTesting
    public static void initializeModel(final TestModelContainer model) {
        model.addInput(TopicInput.class, new TopicInputHandler()).withName("creek/kafka-topic");
        model.addExpectation(TopicExpectation.class, new TopicExpectationHandler())
                .withName("creek/kafka-topic");
    }

    private static class TopicExpectationHandler implements ExpectationHandler<TopicExpectation> {
        @Override
        public Verifier prepare(final Collection<TopicExpectation> expectations) {
            return null;
        }
    }
}
