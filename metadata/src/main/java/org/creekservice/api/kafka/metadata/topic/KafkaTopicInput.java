/*
 * Copyright 2021-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.metadata.topic;

import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.UnownedResource;

/**
 * An input topic that is not owned.
 *
 * <p>i.e. an input topic that was created from an owned output topic via {@link
 * OwnedKafkaTopicOutput#toInput()}.
 *
 * <p>Most input topics are not conceptually owned by the components that use them. However, some
 * components may have owned input topics, which should be defined using {@link
 * OwnedKafkaTopicInput}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KafkaTopicInput<K, V>
        extends ComponentInput, UnownedResource, KafkaTopicDescriptor<K, V> {}
