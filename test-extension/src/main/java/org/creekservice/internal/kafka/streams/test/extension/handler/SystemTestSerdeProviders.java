/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider.SystemTestSerde;

/** Discovers and initializes system test serde providers available at runtime. */
public final class SystemTestSerdeProviders {

    private final Map<SerializationFormat, SystemTestSerde> providers;

    /**
     * Create providers by discovering {@link KafkaSystemTestSerdeProvider} implementations via
     * {@link ServiceLoader}.
     *
     * @return the discovered providers.
     */
    public static SystemTestSerdeProviders create() {
        return create(ServiceLoader.load(KafkaSystemTestSerdeProvider.class));
    }

    @VisibleForTesting
    static SystemTestSerdeProviders create(
            final Iterable<KafkaSystemTestSerdeProvider> providerLoader) {
        final Map<SerializationFormat, KafkaSystemTestSerdeProvider> loaded = load(providerLoader);
        return new SystemTestSerdeProviders(loaded);
    }

    @VisibleForTesting
    SystemTestSerdeProviders(
            final Map<SerializationFormat, KafkaSystemTestSerdeProvider> providers) {
        this.providers =
                requireNonNull(providers, "providers").entrySet().stream()
                        .collect(toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().create()));
    }

    TestKafkaTopic get(final KafkaTopicDescriptor<?, ?> descriptor) {
        return new TestKafkaTopic(
                descriptor,
                serdeFactory(descriptor.key().format()),
                serdeFactory(descriptor.value().format()));
    }

    private SystemTestSerde serdeFactory(final SerializationFormat format) {
        final SystemTestSerde found = providers.get(format);
        if (found == null) {
            throw new UnknownSerializationFormatException(format);
        }
        return found;
    }

    private static Map<SerializationFormat, KafkaSystemTestSerdeProvider> load(
            final Iterable<KafkaSystemTestSerdeProvider> providerLoader) {
        final Map<SerializationFormat, KafkaSystemTestSerdeProvider> result = new HashMap<>();
        for (final KafkaSystemTestSerdeProvider provider : providerLoader) {
            final KafkaSystemTestSerdeProvider existing = result.put(provider.format(), provider);
            if (existing != null) {
                throw new IllegalStateException(
                        "Encountered a second system test serde provider for a previously seen"
                                + " format. There can be only one provider for each format at"
                                + " runtime. format="
                                + provider.format()
                                + ", providers={"
                                + existing
                                + ", "
                                + provider
                                + "}");
            }
        }
        return Map.copyOf(result);
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
