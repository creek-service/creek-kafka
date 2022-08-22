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

package org.creekservice.api.kafka.metadata;


import java.net.URI;
import java.net.URISyntaxException;

/** Util class for generating consistent URIs for Kafka resource descriptors. */
public final class KafkaResourceIds {

    public static final String TOPIC_SCHEME = "kafka-topic";

    private KafkaResourceIds() {}

    public static URI topicId(final String clusterName, final String topicName) {
        try {
            return new URI(TOPIC_SCHEME, clusterName, "/" + topicName, null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
