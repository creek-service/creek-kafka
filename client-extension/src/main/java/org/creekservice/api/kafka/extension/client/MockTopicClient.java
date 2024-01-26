/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.extension.client;

import java.util.List;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;

/**
 * A mock {@link TopicClient} interface to help with testing.
 *
 * <p>This mock interface defaults many of the base types methods, minimising the code changes
 * needed when upgrading Creek.
 *
 * <p>Register the mock client during testing so that no external Kafka cluster is required. The
 * mock is normally installed vis {@link
 * org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions#testBuilder()}:
 *
 * <pre>
 * CreekServices.builder(new MyServiceDescriptor())
 *    .with(KafkaClientsExtensionOptions.testBuilder().build())
 *    .build();
 * </pre>
 *
 * <p>Or a custom client can be installed:
 *
 * <pre>
 * CreekServices.builder(new MyServiceDescriptor())
 *    .with(KafkaClientsExtensionOptions.builder()
 *        .withTypeOverride(TopicClient.Factory.class, CustomTopicClient::new)
 *        .build())
 *    .build();
 * </pre>
 */
public interface MockTopicClient extends TopicClient {

    @Override
    default void ensureTopicsExist(List<? extends CreatableKafkaTopic<?, ?>> topics) {
        // No-op.
    }
}
