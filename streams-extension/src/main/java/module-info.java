/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionProvider;
import org.creekservice.api.service.extension.CreekExtensionProvider;

/** A service extension that provides functionality to work with Kafka Streams apps. */
module creek.kafka.streams.extension {
    requires transitive creek.kafka.clients.extension;
    requires transitive kafka.streams;
    requires creek.observability.logging;
    requires creek.observability.lifecycle;
    requires creek.base.type;
    requires com.github.spotbugs.annotations;

    exports org.creekservice.api.kafka.streams.extension;
    exports org.creekservice.api.kafka.streams.extension.exception;
    exports org.creekservice.api.kafka.streams.extension.observation;
    exports org.creekservice.api.kafka.streams.extension.util;

    provides CreekExtensionProvider with
            KafkaStreamsExtensionProvider;
}
