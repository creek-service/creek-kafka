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

package org.creekservice.api.kafka.streams.extension.util;

import org.apache.kafka.streams.kstream.Named;

/**
 * Annoyingly, can't access the actual text in the {@link Named} instance without using internals.
 */
public final class NamedAccessor {

    private NamedAccessor() {}

    public static String text(final Named named) {
        return new NamedInternal(named).name();
    }

    private static final class NamedInternal extends Named {
        NamedInternal(final Named internal) {
            super(internal);
        }

        public String name() {
            return name;
        }
    }
}
