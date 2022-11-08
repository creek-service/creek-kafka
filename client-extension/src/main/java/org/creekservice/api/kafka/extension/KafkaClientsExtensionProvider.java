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

package org.creekservice.api.kafka.extension;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.client.TopicClient;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.service.extension.CreekExtensionProvider;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.component.model.ComponentModelContainer.HandlerTypeRef;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.extension.client.KafkaTopicClient;
import org.creekservice.internal.kafka.extension.config.ClustersPropertiesFactory;
import org.creekservice.internal.kafka.extension.resource.KafkaResourceValidator;
import org.creekservice.internal.kafka.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.extension.resource.ResourceRegistryFactory;
import org.creekservice.internal.kafka.extension.resource.TopicResourceHandler;

/** Provider of {@link KafkaClientsExtension}. */
public final class KafkaClientsExtensionProvider
        implements CreekExtensionProvider<KafkaClientsExtension> {

    private final ExtensionFactory extensionFactory;
    private final ResourceRegistryFactory resourcesFactory;
    private final TopicClientFactory topicClientFactory;
    private final ClustersPropertiesFactory propertiesFactory;
    private final KafkaResourceValidator resourceValidator;

    /** Constructor */
    public KafkaClientsExtensionProvider() {
        this(
                new KafkaResourceValidator(),
                new ClustersPropertiesFactory(),
                KafkaTopicClient::new,
                ClientsExtension::new,
                new ResourceRegistryFactory());
    }

    @VisibleForTesting
    KafkaClientsExtensionProvider(
            final KafkaResourceValidator resourceValidator,
            final ClustersPropertiesFactory propertiesFactory,
            final TopicClientFactory topicClientFactory,
            final ExtensionFactory extensionFactory,
            final ResourceRegistryFactory resourcesFactory) {
        this.resourceValidator = requireNonNull(resourceValidator, "kafkaResourceValidator");
        this.propertiesFactory = requireNonNull(propertiesFactory, "configFactory");
        this.topicClientFactory = requireNonNull(topicClientFactory, "topicClientFactory");
        this.extensionFactory = requireNonNull(extensionFactory, "extensionFactory");
        this.resourcesFactory = requireNonNull(resourcesFactory, "resourcesFactory");
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
        final ResourceRegistry resources = resourcesFactory.create(components, properties);
        final TopicClient topicClient =
                options.topicClient().orElseGet(() -> topicClientFactory.create(properties));

        api.components()
                .model()
                .addResource(new HandlerTypeRef<>() {}, new TopicResourceHandler(topicClient));

        return extensionFactory.create(properties, resources);
    }

    @VisibleForTesting
    interface ExtensionFactory {
        ClientsExtension create(ClustersProperties clustersProperties, ResourceRegistry resources);
    }

    @VisibleForTesting
    interface TopicClientFactory {

        TopicClient create(ClustersProperties properties);
    }
}
