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

package org.creekservice.internal.kafka.streams.test.extension.yaml;


import com.fasterxml.jackson.core.JsonParser;
import java.io.File;
import java.net.URI;

public final class JsonLocation {

    private static final URI UNKNOWN = URI.create("unknown");

    private JsonLocation() {}

    public static URI location(final JsonParser parser) {
        return location(parser.currentLocation());
    }

    public static URI location(final com.fasterxml.jackson.core.JsonLocation location) {
        final Object content = location.contentReference().getRawContent();
        if (!(content instanceof File)) {
            return UNKNOWN;
        }

        final String filePath =
                ((File) content).toURI().toString().replaceFirst("file:/", "file:///");

        return URI.create(filePath + ":" + location.getLineNr());
    }
}