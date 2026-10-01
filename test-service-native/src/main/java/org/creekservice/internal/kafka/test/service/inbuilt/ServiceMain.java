/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.test.service.inbuilt;

import java.util.Map;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.test.service.inbuilt.NativeServiceDescriptor;
import org.creekservice.api.kafka.test.service.inbuilt.OwnedAToOwnedBServiceDescriptor;
import org.creekservice.api.kafka.test.service.inbuilt.OwnedAToUnownedBServiceDescriptor;
import org.creekservice.api.kafka.test.service.inbuilt.OwnedBToOwnedCServiceDescriptor;
import org.creekservice.api.kafka.test.service.inbuilt.PipelineServiceDescriptor;
import org.creekservice.api.kafka.test.service.inbuilt.UnownedBToOwnedCServiceDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.creekservice.internal.kafka.test.service.inbuilt.kafka.streams.PipelineTopology;
import org.creekservice.internal.kafka.test.service.inbuilt.kafka.streams.TopologyBuilder;

public final class ServiceMain {

    private static final Map<String, ServiceDescriptor> SERVICE_DESCRIPTORS =
            Map.of(
                    "native-service", new NativeServiceDescriptor(),
                    "owned-a-to-owned-b-service", new OwnedAToOwnedBServiceDescriptor(),
                    "unowned-b-to-owned-c-service", new UnownedBToOwnedCServiceDescriptor(),
                    "owned-a-to-unowned-b-service", new OwnedAToUnownedBServiceDescriptor(),
                    "owned-b-to-owned-c-service", new OwnedBToOwnedCServiceDescriptor());

    private ServiceMain() {}

    public static void main(final String... args) {
        final String name =
                System.getenv().getOrDefault("KAFKA_DEFAULT_APPLICATION_ID", "native-service");
        final ServiceDescriptor descriptor = SERVICE_DESCRIPTORS.get(name);
        if (descriptor == null) {
            throw new IllegalArgumentException("Unknown fixture service: " + name);
        }
        try (CreekContext context = CreekServices.context(descriptor)) {
            final KafkaStreamsExtension ext = context.extension(KafkaStreamsExtension.class);
            if (descriptor instanceof PipelineServiceDescriptor pipeline) {
                ext.execute(PipelineTopology.build(ext, pipeline));
            } else {
                ext.execute(new TopologyBuilder(ext).build());
            }
        }
    }
}
