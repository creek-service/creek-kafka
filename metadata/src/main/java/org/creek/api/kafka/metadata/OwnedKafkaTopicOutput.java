/*
 * Copyright 2021-2022 Creek Contributors (https://github.com/creek-service)
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

package org.creek.api.kafka.metadata;


import org.creek.api.platform.metadata.ComponentOutput;

/**
 * An output topic that is owned by the component.
 *
 * <p>Most output topics are conceptually owned by the components that use them. For such topics use
 * this class. However, some components may have non-owned output topics, which should be defined
 * using {@link KafkaTopicOutput} acquired from an owned input topic via {@link
 * OwnedKafkaTopicInput#toOutput()}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface OwnedKafkaTopicOutput<K, V> extends ComponentOutput, CreatableKafkaTopic<K, V> {

    /**
     * Create an input topic for this output topic.
     *
     * @return the input topic
     */
    @SuppressWarnings("DuplicatedCode")
    default KafkaTopicInput<K, V> toInput() {
        final OwnedKafkaTopicOutput<K, V> output = this;
        return new KafkaTopicInput<>() {
            @Override
            public String name() {
                return output.name();
            }

            @Override
            public PartDescriptor<K> key() {
                return output.key();
            }

            @Override
            public PartDescriptor<V> value() {
                return output.value();
            }
        };
    }
}
