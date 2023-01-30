/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.streams.extension.observation;

import java.time.Duration;

/** Observer called as the Kafka Streams app transitions through different states. */
public interface LifecycleObserver {

    /** Enum defining the possible exit codes */
    enum ExitCode {
        /** The app shutdown gracefully without the timeout. */
        OK(0),

        /** An exception was thrown starting up the app. */
        EXCEPTION_THROWN_STARTING(-1),

        /** An unhandled exception occurred or the app entered the ERROR state. */
        STREAM_APP_FAILED(-2),

        /** Timed-out closing the streams app. */
        STREAMS_TIMED_OUT_CLOSING(-3),

        /** An exception was thrown during the shutdown process. */
        EXCEPTION_THROWN_STOPPING(-4);

        private final int code;

        ExitCode(final int code) {
            this.code = code;
        }

        /**
         * @return the int value of the exit code.
         */
        public int asInt() {
            return code;
        }

        @Override
        public String toString() {
            return super.toString() + "(" + code + ")";
        }
    }

    /** The app is starting up. */
    default void starting() {}

    /**
     * The app is started.
     *
     * <p>Fires the first time the app enters the {@link #running} state.
     */
    default void started() {}

    /** The app is rebalancing. */
    default void rebalancing() {}

    /** The app is running normally. */
    default void running() {}

    /**
     * The app has failed.
     *
     * <p>Caused either by an unhandled exception on the main thread or an unhandled exception in
     * the streams app.
     *
     * @param e the unhanded exception.
     */
    default void failed(final Throwable e) {}

    /**
     * The app is stopping.
     *
     * <p>Either due to failure or because the JVM has been asked to stop.
     *
     * @param exitCode the initial {@link ExitCode}, which can change if the app fails to stop.
     */
    default void stopping(final ExitCode exitCode) {}

    /**
     * The app timed-out when stopping.
     *
     * @param closeTimeout the configured timeout.
     */
    default void stopTimedOut(final Duration closeTimeout) {}

    /**
     * The app threw an exception while stopping.
     *
     * @param cause the exception thrown.
     */
    default void stopFailed(final Throwable cause) {}

    /**
     * The app has stopped.
     *
     * @param exitCode the exit code.
     */
    default void stopped(final ExitCode exitCode) {}
}
