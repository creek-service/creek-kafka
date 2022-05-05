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

import java.util.Optional;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.api.service.extension.CreekExtensionBuilder;
import org.creekservice.api.service.extension.CreekExtensionOptions;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistryFactory;

/** Builder for the {@link org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension}. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class KafkaStreamsExtensionBuilder implements CreekExtensionBuilder {

    private final BuilderFactory builderFactory;
    private final ExecutorFactory executorFactory;
    private final ExtensionFactory extensionFactory;
    private final ResourceRegistryFactory resourcesFactory;
    private Optional<KafkaStreamsExtensionOptions> options = Optional.empty();

    public KafkaStreamsExtensionBuilder() {
        this(
                KafkaStreamsBuilder::new,
                KafkaStreamsExecutor::new,
                StreamsExtension::new,
                new ResourceRegistryFactory());
    }

    @VisibleForTesting
    KafkaStreamsExtensionBuilder(
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
    public String name() {
        return StreamsExtension.NAME;
    }

    @Override
    public boolean handles(final ResourceDescriptor resourceDef) {
        return resourceDef instanceof KafkaTopicDescriptor;
    }

    @Override
    public boolean with(final CreekExtensionOptions options) {
        if (!(options instanceof KafkaStreamsExtensionOptions)) {
            return false;
        }

        if (this.options.isPresent()) {
            throw new IllegalStateException(
                    KafkaStreamsExtensionOptions.class.getSimpleName() + " can only be set once");
        }

        this.options = Optional.of((KafkaStreamsExtensionOptions) options);
        return true;
    }

    @Override
    public StreamsExtension build(final ComponentDescriptor component) {

        final KafkaStreamsExtensionOptions opts =
                options.orElseGet(() -> KafkaStreamsExtensionOptions.builder().build());

        final ResourceRegistry resources = resourcesFactory.create(component, opts);

        return extensionFactory.create(
                opts, resources, builderFactory.create(opts), executorFactory.create(opts));
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
