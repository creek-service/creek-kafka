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

package org.creekservice.internal.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.service.extension.CreekExtensionProvider;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.model.ResourceHandler;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistryFactory;

/** Provider of {@link org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension}. */
public final class KafkaStreamsExtensionProvider implements CreekExtensionProvider {

    private final BuilderFactory builderFactory;
    private final ExecutorFactory executorFactory;
    private final ExtensionFactory extensionFactory;
    private final ResourceRegistryFactory resourcesFactory;

    public KafkaStreamsExtensionProvider() {
        this(
                KafkaStreamsBuilder::new,
                KafkaStreamsExecutor::new,
                StreamsExtension::new,
                new ResourceRegistryFactory());
    }

    @VisibleForTesting
    KafkaStreamsExtensionProvider(
            final BuilderFactory builderFactory,
            final ExecutorFactory executorFactory,
            final ExtensionFactory extensionFactory,
            final ResourceRegistryFactory resourcesFactory) {
        this.builderFactory = requireNonNull(builderFactory, "builderFactory");
        this.executorFactory = requireNonNull(executorFactory, "executorFactory");
        this.extensionFactory = requireNonNull(extensionFactory, "extensionFactory");
        this.resourcesFactory = requireNonNull(resourcesFactory, "resourcesFactory");
    }

    @Override
    public StreamsExtension initialize(final CreekService api) {
        api.model().addResource(KafkaTopicDescriptor.class, new ResourceHandler<>() {});

        final KafkaStreamsExtensionOptions options =
                api.options()
                        .get(KafkaStreamsExtensionOptions.class)
                        .orElseGet(() -> KafkaStreamsExtensionOptions.builder().build());

        final ResourceRegistry resources = resourcesFactory.create(api.service(), options);

        return extensionFactory.create(
                options,
                resources,
                builderFactory.create(options),
                executorFactory.create(options));
    }

    @VisibleForTesting
    interface BuilderFactory {
        KafkaStreamsBuilder create(KafkaStreamsExtensionOptions options);
    }

    @VisibleForTesting
    interface ExecutorFactory {
        KafkaStreamsExecutor create(KafkaStreamsExtensionOptions options);
    }

    @VisibleForTesting
    interface ExtensionFactory {
        StreamsExtension create(
                KafkaStreamsExtensionOptions options,
                ResourceRegistry resources,
                KafkaStreamsBuilder builder,
                KafkaStreamsExecutor executor);
    }
}
