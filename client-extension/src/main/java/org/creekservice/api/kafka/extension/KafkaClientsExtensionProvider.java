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

package org.creekservice.api.kafka.extension;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.config.TypeOverrides;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.service.extension.CreekExtensionProvider;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.component.model.ComponentModelContainer.HandlerTypeRef;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.extension.config.ClustersPropertiesFactory;
import org.creekservice.internal.kafka.extension.resource.KafkaResourceValidator;
import org.creekservice.internal.kafka.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.extension.resource.TopicRegistrar;
import org.creekservice.internal.kafka.extension.resource.TopicRegistry;
import org.creekservice.internal.kafka.extension.resource.TopicResourceHandler;

/** Provider of {@link KafkaClientsExtension}. */
public final class KafkaClientsExtensionProvider
        implements CreekExtensionProvider<KafkaClientsExtension> {

    private final KafkaResourceValidator resourceValidator;
    private final ClustersPropertiesFactory propertiesFactory;
    private final ResourceRegistry resourceRegistry;
    private final HandlerFactory handlerFactory;
    private final ExtensionFactory extensionFactory;

    /** Constructor */
    public KafkaClientsExtensionProvider() {
        this(
                new KafkaResourceValidator(),
                new ClustersPropertiesFactory(),
                new ResourceRegistry(),
                TopicResourceHandler::new,
                ClientsExtension::new);
    }

    @VisibleForTesting
    KafkaClientsExtensionProvider(
            final KafkaResourceValidator resourceValidator,
            final ClustersPropertiesFactory propertiesFactory,
            final ResourceRegistry resourceRegistry,
            final HandlerFactory handlerFactory,
            final ExtensionFactory extensionFactory) {
        this.resourceValidator = requireNonNull(resourceValidator, "kafkaResourceValidator");
        this.propertiesFactory = requireNonNull(propertiesFactory, "configFactory");
        this.resourceRegistry = requireNonNull(resourceRegistry, "resourceRegistry");
        this.handlerFactory = requireNonNull(handlerFactory, "handlerFactory");
        this.extensionFactory = requireNonNull(extensionFactory, "extensionFactory");
    }

    @Override
    public KafkaClientsExtension initialize(final CreekService api) {
        final List<ComponentDescriptor> components =
                api.components().descriptors().stream().collect(Collectors.toList());

        resourceValidator.validate(components.stream());

        final ClientsExtensionOptions options =
                api.options()
                        .get(ClientsExtensionOptions.class)
                        .orElseGet(() -> KafkaClientsExtensionOptions.builder().build());

        final ClustersProperties properties = propertiesFactory.create(components, options);

        api.components()
                .model()
                .addResource(
                        new HandlerTypeRef<>() {},
                        handlerFactory.create(
                                options.typeOverrides(), resourceRegistry, properties));

        return extensionFactory.create(properties, resourceRegistry);
    }

    @VisibleForTesting
    interface HandlerFactory {
        TopicResourceHandler create(
                TypeOverrides typeOverrides,
                TopicRegistrar resources,
                ClustersProperties properties);
    }

    @VisibleForTesting
    interface ExtensionFactory {
        ClientsExtension create(ClustersProperties clustersProperties, TopicRegistry resources);
    }
}
