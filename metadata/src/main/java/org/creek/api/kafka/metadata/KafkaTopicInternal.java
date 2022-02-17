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


import org.creek.api.platform.metadata.ComponentInternal;

/**
 * An internal Kafka topic, e.g. a repartition or changelog topic
 *
 * <p>Internal topics are created by the owning service, but not by service initialization code. The
 * service, or more commonly Kafka Streams, will explicitly create them. For such topics, this is
 * the right type to implement. For internal topics that should be created by service initialization
 * code, use {@link CreatableKafkaTopicInternal}
 *
 * <p>Schemas for internal topics <i>are</i> automatically registered by the service initialization
 * code.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KafkaTopicInternal<K, V> extends ComponentInternal, KafkaTopic<K, V> {}
