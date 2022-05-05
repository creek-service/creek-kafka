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


import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;

module creek.kafka.serde {
    requires transitive creek.kafka.metadata;
    requires transitive creek.base.annotation;
    requires transitive kafka.clients;

    exports org.creekservice.api.kafka.serde;
    exports org.creekservice.api.kafka.serde.provider;

    uses KafkaSerdeProvider;

    provides KafkaSerdeProvider with
            org.creekservice.api.kafka.serde.NativeKafkaSerdeProvider;
}
