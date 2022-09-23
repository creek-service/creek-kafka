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

package org.creekservice.api.kafka.extension.resource;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;

/**
 * A Kafka topic resource.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KafkaTopic<K, V> {

    /** @return the name of the topic */
    String name();

    /** @return the serde used to (de)serialize the keys stored in the topic */
    Serde<K> keySerde();

    /** @return the serde used to (de)serialize the values stored in the topic */
    Serde<V> valueSerde();

    /** @return a Kafka producer configured to produce to this topic */
    Producer<K, V> producer();

    /** @return a Kafka consumer configured to consume from this topic */
    Consumer<K, V> consumer();
}
