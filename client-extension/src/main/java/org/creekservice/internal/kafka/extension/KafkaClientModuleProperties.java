/*
 * Copyright 2023-2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Provides access to module-level properties baked in at build time. */
public final class KafkaClientModuleProperties {

    private static final String PROPERTIES_FILE = "creek-kafka-clients-extension.properties";
    private static final String CONFLUENT_VERSION = loadConfluentVersion();

    private KafkaClientModuleProperties() {}

    /**
     * @return the Confluent platform version baked in at build time.
     */
    public static String confluentVersion() {
        return CONFLUENT_VERSION;
    }

    private static String loadConfluentVersion() {
        try (InputStream is =
                KafkaClientModuleProperties.class
                        .getClassLoader()
                        .getResourceAsStream(PROPERTIES_FILE)) {
            if (is == null) {
                throw new IllegalStateException(
                        "Resource not found on classpath: " + PROPERTIES_FILE);
            }
            final Properties props = new Properties();
            props.load(is);
            final String version = props.getProperty("confluentVersion");
            if (version == null || version.isBlank()) {
                throw new IllegalStateException("confluentVersion not set in " + PROPERTIES_FILE);
            }
            return version;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load " + PROPERTIES_FILE, e);
        }
    }
}
