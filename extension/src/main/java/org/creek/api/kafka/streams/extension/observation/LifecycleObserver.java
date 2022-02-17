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

package org.creek.api.kafka.streams.extension.observation;


import java.time.Duration;

/** Observer called as the Kafka Streams app transitions through different states. */
public interface LifecycleObserver {

    default void starting() {}

    default void rebalancing() {}

    default void running() {}

    default void started() {}

    default void failed() {}

    default void stopping(final int exitCode) {}

    default void stopTimedOut(final Duration closeTimeout) {}

    default void stopFailed(Throwable cause) {}

    default void stopped(final int exitCode) {}
}
