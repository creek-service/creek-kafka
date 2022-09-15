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

import java.util.List;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.kafka.streams.extension.client.TopicClient;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.service.extension.CreekExtensionProvider;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.component.model.ComponentModelContainer.HandlerTypeRef;
import org.creekservice.internal.kafka.common.resource.KafkaResourceValidator;
import org.creekservice.internal.kafka.streams.extension.client.KafkaTopicClient;
import org.creekservice.internal.kafka.streams.extension.config.ClustersPropertiesFactory;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistryFactory;
import org.creekservice.internal.kafka.streams.extension.resource.TopicResourceHandler;

/** Provider of {@link org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension}. */
public final class KafkaStreamsExtensionProvider
        implements CreekExtensionProvider<KafkaStreamsExtension> {

    private final BuilderFactory builderFactory;
    private final ExecutorFactory executorFactory;
    private final ExtensionFactory extensionFactory;
    private final ResourceRegistryFactory resourcesFactory;
    private final TopicClientFactory topicClientFactory;
    private final ClustersPropertiesFactory propertiesFactory;
    private final KafkaResourceValidator resourceValidator;

    public KafkaStreamsExtensionProvider() {
        this(
                new KafkaResourceValidator(),
                new ClustersPropertiesFactory(),
                KafkaTopicClient::new,
                KafkaStreamsBuilder::new,
                KafkaStreamsExecutor::new,
                StreamsExtension::new,
                new ResourceRegistryFactory());
    }

    @VisibleForTesting
    KafkaStreamsExtensionProvider(
            final KafkaResourceValidator resourceValidator,
            final ClustersPropertiesFactory propertiesFactory,
            final TopicClientFactory topicClientFactory,
            final BuilderFactory builderFactory,
            final ExecutorFactory executorFactory,
            final ExtensionFactory extensionFactory,
            final ResourceRegistryFactory resourcesFactory) {
        this.resourceValidator = requireNonNull(resourceValidator, "kafkaResourceValidator");
        this.propertiesFactory = requireNonNull(propertiesFactory, "configFactory");
        this.topicClientFactory = requireNonNull(topicClientFactory, "topicClientFactory");
        this.builderFactory = requireNonNull(builderFactory, "builderFactory");
        this.executorFactory = requireNonNull(executorFactory, "executorFactory");
        this.extensionFactory = requireNonNull(extensionFactory, "extensionFactory");
        this.resourcesFactory = requireNonNull(resourcesFactory, "resourcesFactory");
    }

    @Override
    public KafkaStreamsExtension initialize(final CreekService api) {
        final List<ComponentDescriptor> components =
                api.components().descriptors().stream().collect(Collectors.toList());

        resourceValidator.validate(components.stream());

        final KafkaStreamsExtensionOptions options =
                api.options()
                        .get(KafkaStreamsExtensionOptions.class)
                        .orElseGet(() -> KafkaStreamsExtensionOptions.builder().build());

        final ClustersProperties properties = propertiesFactory.create(components, options);
        final ResourceRegistry resources = resourcesFactory.create(components, properties);
        final TopicClient topicClient =
                options.topicClient().orElseGet(() -> topicClientFactory.create(properties));

        final KafkaStreamsBuilder builder = builderFactory.create(properties);
        final KafkaStreamsExecutor executor = executorFactory.create(options);

        api.components()
                .model()
                .addResource(new HandlerTypeRef<>() {}, new TopicResourceHandler(topicClient));

        return extensionFactory.create(properties, resources, builder, executor);
    }

    @VisibleForTesting
    interface BuilderFactory {
        KafkaStreamsBuilder create(ClustersProperties clustersProperties);
    }

    @VisibleForTesting
    interface ExecutorFactory {
        KafkaStreamsExecutor create(KafkaStreamsExtensionOptions options);
    }

    @VisibleForTesting
    interface ExtensionFactory {
        StreamsExtension create(
                ClustersProperties clustersProperties,
                ResourceRegistry resources,
                KafkaStreamsBuilder builder,
                KafkaStreamsExecutor executor);
    }

    @VisibleForTesting
    interface TopicClientFactory {

        TopicClient create(ClustersProperties properties);
    }
}
