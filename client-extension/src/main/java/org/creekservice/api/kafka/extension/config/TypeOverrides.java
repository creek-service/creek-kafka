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

package org.creekservice.api.kafka.extension.config;

import java.util.Optional;

/**
 * Tracks any custom types registered to override default types used internally by the extension.
 */
public interface TypeOverrides {

    /**
     * Retrieve the override instance for the supplied {@code type}, if one is set.
     *
     * @param type the type to look up. Must match the type passed to {@link
     *     org.creekservice.api.kafka.extension.ClientsExtensionOptions.Builder#withTypeOverride}.
     * @return the instance to use, if set, otherwise {@link Optional#empty()}.
     * @param <T> the type to look up.
     */
    <T> Optional<T> get(Class<T> type);
}
