/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.service.extension.CreekExtensionProvider;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.internal.kafka.streams.extension.KafkaStreamsBuilder;
import org.creekservice.internal.kafka.streams.extension.KafkaStreamsExecutor;
import org.creekservice.internal.kafka.streams.extension.StreamsExtension;

/** Provider of {@link KafkaStreamsExtension}. */
public final class KafkaStreamsExtensionProvider
        implements CreekExtensionProvider<KafkaStreamsExtension> {

    private final BuilderFactory builderFactory;
    private final ExecutorFactory executorFactory;
    private final ExtensionFactory extensionFactory;

    /** Constructor */
    @SuppressWarnings("unused") // Invoked via reflection
    public KafkaStreamsExtensionProvider() {
        this(KafkaStreamsBuilder::new, KafkaStreamsExecutor::new, StreamsExtension::new);
    }

    @VisibleForTesting
    KafkaStreamsExtensionProvider(
            final BuilderFactory builderFactory,
            final ExecutorFactory executorFactory,
            final ExtensionFactory extensionFactory) {
        this.builderFactory = requireNonNull(builderFactory, "builderFactory");
        this.executorFactory = requireNonNull(executorFactory, "executorFactory");
        this.extensionFactory = requireNonNull(extensionFactory, "extensionFactory");
    }

    @Override
    public KafkaStreamsExtension initialize(final CreekService api) {
        final KafkaStreamsExtensionOptions options = options(api);

        final KafkaClientsExtension clientExtension =
                api.extensions().ensureExtension(KafkaClientsExtensionProvider.class);

        final KafkaStreamsBuilder builder = builderFactory.create(clientExtension);
        final KafkaStreamsExecutor executor = executorFactory.create(options);

        return extensionFactory.create(clientExtension, builder, executor);
    }

    private KafkaStreamsExtensionOptions options(final CreekService api) {
        if (api.options().get(KafkaClientsExtensionOptions.class).isPresent()) {
            throw new IllegalArgumentException(
                    "KafkaClientsExtensionOptions detected: use KafkaStreamsExtensionOptions for"
                            + " streams based apps");
        }

        return api.options()
                .get(KafkaStreamsExtensionOptions.class)
                .orElseGet(() -> KafkaStreamsExtensionOptions.builder().build());
    }

    @VisibleForTesting
    interface BuilderFactory {
        KafkaStreamsBuilder create(KafkaClientsExtension clientsExtension);
    }

    @VisibleForTesting
    interface ExecutorFactory {
        KafkaStreamsExecutor create(KafkaStreamsExtensionOptions options);
    }

    @VisibleForTesting
    interface ExtensionFactory {
        StreamsExtension create(
                KafkaClientsExtension clientsExtension,
                KafkaStreamsBuilder builder,
                KafkaStreamsExecutor executor);
    }
}
