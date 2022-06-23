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


import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.CreekTestExtension;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StreamsTestLifecycleListener;

/**
 * A Creek system test extension for testing Kafka Streams based microservices.
 *
 * <p>The extension will start any required Kafka clusters and handle any Kafka related inputs and
 * expectations defined in system tests.
 */
public final class KafkaStreamsTestExtension implements CreekTestExtension {
    @Override
    public String name() {
        return "creek-kafka-streams";
    }

    @Override
    public void initialize(final CreekSystemTest systemTest) {
        // Todo:     systemTest.model().addInput(TopicInput.class, handler);
        // Todo:     systemTest.model().addExpectation(TopicExpectation.class, handler);

        systemTest.testSuite().listener().append(new StreamsTestLifecycleListener(systemTest));
    }
}

// Todo: test.