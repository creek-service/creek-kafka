/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.service.extension.CreekExtensionProvider;

/** A service extension that provides functionality to work with Java Kafka client. */
module creek.kafka.clients.extension {
    requires transitive creek.base.annotation;
    requires transitive creek.service.extension;
    requires transitive creek.kafka.metadata;
    requires transitive creek.kafka.serde;
    requires transitive kafka.clients;
    requires creek.base.type;
    requires creek.observability.logging;

    exports org.creekservice.api.kafka.extension;
    exports org.creekservice.api.kafka.extension.client;
    exports org.creekservice.api.kafka.extension.config;
    exports org.creekservice.api.kafka.extension.resource;
    exports org.creekservice.internal.kafka.extension to
            creek.kafka.streams.extension,
            creek.kafka.test.extension;
    exports org.creekservice.internal.kafka.extension.config to
            creek.kafka.streams.extension,
            creek.kafka.test.extension;
    exports org.creekservice.internal.kafka.extension.resource to
            creek.kafka.test.extension;

    provides CreekExtensionProvider with
            KafkaClientsExtensionProvider;
}
