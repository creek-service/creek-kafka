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


import org.creek.api.platform.metadata.ComponentInput;

/**
 * An input topic that is owned by the component.
 *
 * <p>Most input topics are not conceptually owned by the components that use them. For such topics,
 * use {@link KafkaTopicInput} acquired from the associated owned output topic via {@link
 * OwnedKafkaTopicOutput#toInput()} However, some components may have owned input topics, which
 * should be defined using this type.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface OwnedKafkaTopicInput<K, V> extends ComponentInput, CreatableKafkaTopic<K, V> {

    /**
     * Create an output topic for this input topic.
     *
     * @return the output topic.
     */
    @SuppressWarnings("DuplicatedCode")
    default KafkaTopicOutput<K, V> toOutput() {
        final OwnedKafkaTopicInput<K, V> input = this;
        return new KafkaTopicOutput<>() {
            @Override
            public String name() {
                return input.name();
            }

            @Override
            public Class<K> keyType() {
                return input.keyType();
            }

            @Override
            public Class<V> valueType() {
                return input.valueType();
            }
        };
    }
}
