/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.system.test.extension.CreekTestExtension;
import org.creekservice.internal.kafka.streams.test.extension.KafkaTestExtension;

/** A system test extension to enable system testing of microservices that interact with Kafka. */
module creek.kafka.test.extension {
    requires transitive creek.system.test.extension;
    requires transitive com.fasterxml.jackson.annotation;
    requires transitive com.fasterxml.jackson.databind;
    requires creek.kafka.clients.extension;
    requires creek.kafka.metadata;
    requires creek.base.type;
    requires org.slf4j;

    exports org.creekservice.internal.kafka.streams.test.extension.model to
            com.fasterxml.jackson.databind;
    exports org.creekservice.internal.kafka.streams.test.extension.util to
            com.fasterxml.jackson.databind;

    provides CreekTestExtension with
            KafkaTestExtension;
}
