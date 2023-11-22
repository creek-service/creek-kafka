/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.internal.kafka.serde.provider.NativeKafkaSerdeProvider;

/** Utility class to access details on the types the {@link NativeKafkaSerdeProvider} supports. */
public final class NativeKafkaSerde {

    private NativeKafkaSerde() {}

    /**
     * Check if this provider supports the supplied {@code type}.
     *
     * @param type the type to check
     * @return {@code true} if supported, {@code false} otherwise.
     */
    public static boolean supports(final Class<?> type) {
        return NativeKafkaSerdeProvider.supports(type);
    }
}
