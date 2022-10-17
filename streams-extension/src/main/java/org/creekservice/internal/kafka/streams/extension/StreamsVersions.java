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

package org.creekservice.internal.kafka.streams.extension;

import static org.creekservice.api.base.type.JarVersion.jarVersion;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.streams.KafkaStreams;

/** Tool for handling different versions of Kafka Streams. */
public final class StreamsVersions {

    public static final int MAJOR_VERSION;
    public static final int MINOR_VERSION;

    static {
        final String version =
                jarVersion(KafkaStreams.class)
                        .orElseThrow(
                                () -> new IllegalStateException("Kafka Streams jar not found"));

        final Matcher matcher = Pattern.compile("(\\d+)\\.(\\d+)\\..*").matcher(version);
        if (!matcher.matches()) {
            throw new IllegalStateException(
                    "Kafka Streams jar version not in expected form. was: " + version);
        }

        MAJOR_VERSION = Integer.parseInt(matcher.group(1));
        MINOR_VERSION = Integer.parseInt(matcher.group(2));
    }

    public static final String EXACTLY_ONCE_V2 =
            versionAtLeast(3, 1).map(v -> "exactly_once_v2").orElse("exactly_once_beta");

    private static Optional<?> versionAtLeast(final int minMajor, final int minMinor) {
        return (MAJOR_VERSION > minMajor)
                        || (MAJOR_VERSION == minMajor && MINOR_VERSION >= minMinor)
                ? Optional.of("")
                : Optional.empty();
    }

    private StreamsVersions() {}
}
