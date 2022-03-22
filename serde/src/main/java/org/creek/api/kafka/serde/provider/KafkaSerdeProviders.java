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

package org.creek.api.kafka.serde.provider;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import org.creek.api.base.annotation.VisibleForTesting;
import org.creek.api.kafka.metadata.SerializationFormat;
import org.creek.internal.kafka.serde.provider.ProviderLoader;

/** Discovers Kafka serde providers available at runtime. */
public final class KafkaSerdeProviders {

    private final Map<SerializationFormat, KafkaSerdeProvider> providers;

    public static KafkaSerdeProviders create() {
        return new KafkaSerdeProviders(new ProviderLoader().load());
    }

    @VisibleForTesting
    KafkaSerdeProviders(final Map<SerializationFormat, KafkaSerdeProvider> providers) {
        this.providers = Map.copyOf(requireNonNull(providers, "providers"));
    }

    public KafkaSerdeProvider get(final SerializationFormat format) {
        final KafkaSerdeProvider found = providers.get(format);
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
