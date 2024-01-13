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

package org.creekservice.internal.kafka.serde.provider;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;

/** Loader of Kafka Serde providers */
public final class ProviderLoader {
    private final LoaderFactory factory;

    /** Constructor */
    public ProviderLoader() {
        this(ServiceLoader::load);
    }

    @VisibleForTesting
    ProviderLoader(final LoaderFactory factory) {
        this.factory = requireNonNull(factory, "factory");
    }

    /**
     * Load all serde providers from the class and module path.
     *
     * @return the discovered serde providers.
     */
    public Map<SerializationFormat, KafkaSerdeProvider> load() {
        return factory.load(KafkaSerdeProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .collect(
                        Collectors.toUnmodifiableMap(
                                KafkaSerdeProvider::format, Function.identity(), throwingMerger()));
    }

    private static BinaryOperator<KafkaSerdeProvider> throwingMerger() {
        return (providerA, providerB) -> {
            throw new IllegalStateException(
                    "Encountered a second Kafka serde provider for a previously seen format. "
                            + "There can be only one provider for each format at runtime."
                            + " format="
                            + providerA.format()
                            + ", providers={"
                            + providerA
                            + ", "
                            + providerB
                            + "}");
        };
    }

    @VisibleForTesting
    interface LoaderFactory {
        <T> ServiceLoader<T> load(Class<T> type);
    }
}
