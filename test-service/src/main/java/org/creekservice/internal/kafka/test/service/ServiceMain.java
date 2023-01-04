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

package org.creekservice.internal.kafka.test.service;

import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.test.service.TestServiceDescriptor;
import org.creekservice.api.service.context.CreekServices;
import org.creekservice.internal.kafka.test.service.kafka.streams.TopologyBuilder;

public final class ServiceMain {

    private ServiceMain() {}

    public static void main(final String... args) {
        final KafkaStreamsExtension ext =
                CreekServices.context(new TestServiceDescriptor())
                        .extension(KafkaStreamsExtension.class);

        ext.execute(new TopologyBuilder(ext).build());
    }
}
