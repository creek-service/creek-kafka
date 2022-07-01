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

package org.creekservice.api.kafka.common.config;


import java.util.Map;
import java.util.stream.Collectors;

public final class SystemEnvPropertyOverrides implements KafkaPropertyOverrides {

    public static final String KAFKA_PROPERTY_PREFIX = "KAFKA_";

    public static KafkaPropertyOverrides systemEnvPropertyOverrides() {
        return new SystemEnvPropertyOverrides();
    }

    private SystemEnvPropertyOverrides() {}

    @Override
    public Map<String, Object> get() {
        return System.getenv().entrySet().stream()
                .filter(e -> e.getKey().startsWith(KAFKA_PROPERTY_PREFIX))
                .collect(Collectors.toMap(e -> varNameToPropName(e.getKey()), Map.Entry::getValue));
    }

    private static String varNameToPropName(final String varName) {
        return varName.substring(KAFKA_PROPERTY_PREFIX.length()).replaceAll("_", ".").toLowerCase();
    }
}
