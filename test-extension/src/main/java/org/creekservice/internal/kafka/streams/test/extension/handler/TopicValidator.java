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

package org.creekservice.internal.kafka.streams.test.extension.handler;


import org.creekservice.api.kafka.extension.resource.KafkaTopic;

/** Validates operations on Kafka topics. */
public interface TopicValidator {

    /**
     * Is it valid for a test to produce to the supplied {@code topic}?
     *
     * <p>The method validates that at least one service-under-test consumes from the supplied
     * {@code topic}.
     *
     * @param topic the topic to validate.
     * @throws RuntimeException if the supplied topic is only ever produced to by the
     *     services-under-test
     */
    void validateCanProduce(KafkaTopic<?, ?> topic);

    /**
     * Is it valid for a test to consume from the supplied {@code topic}?
     *
     * <p>The method validates that at least one service-under-test produces to the supplied {@code
     * topic}.
     *
     * @param topic the topic to validate.
     * @throws RuntimeException if the supplied topic is only ever consumed by the
     *     services-under-test
     */
    void validateCanConsume(KafkaTopic<?, ?> topic);
}
