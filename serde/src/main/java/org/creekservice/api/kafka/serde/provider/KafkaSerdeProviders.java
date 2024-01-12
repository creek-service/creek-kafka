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

package org.creekservice.api.kafka.serde.provider;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Map;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider.SerdeFactory;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.internal.kafka.serde.provider.ProviderLoader;

/** Discovers Kafka serde providers available at runtime. */
public final class KafkaSerdeProviders {

    private final Map<SerializationFormat, SerdeFactory> providers;

    /**
     * Factory method
     *
     * @param api Creek api passed to serde providers to initialize.
     * @return an instance.
     */
    public static KafkaSerdeProviders create(final CreekService api) {
        return new KafkaSerdeProviders(api, new ProviderLoader().load());
    }

    @VisibleForTesting
    KafkaSerdeProviders(
            final CreekService api, final Map<SerializationFormat, KafkaSerdeProvider> providers) {
        this.providers =
                requireNonNull(providers, "providers").entrySet().stream()
                        .collect(
                                toUnmodifiableMap(
                                        Map.Entry::getKey, e -> e.getValue().initialize(api)));
    }

    /**
     * Get a serde provider for a specific {@code format}.
     *
     * @param format the format
     * @return the serde provider.
     */
    public SerdeFactory get(final SerializationFormat format) {
        final SerdeFactory found = providers.get(format);
        if (found == null) {
            throw new UnknownSerializationFormatException(format);
        }

        return found;
    }

    private static final class UnknownSerializationFormatException extends RuntimeException {
        UnknownSerializationFormatException(final SerializationFormat format) {
            super(
                    "Unknown serialization format. Are you missing a runtime serde jar?"
                            + " format="
                            + format);
        }
    }
}
