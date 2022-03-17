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

package org.creek.internal.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.creek.api.kafka.streams.observation.StateRestoreObserver;

final class RestoreListener implements StateRestoreListener {

    private final StateRestoreObserver observer;

    RestoreListener(final StateRestoreObserver observer) {
        this.observer = requireNonNull(observer, "observer");
    }

    @Override
    public void onRestoreStart(
            final TopicPartition topicPartition,
            final String storeName,
            final long startingOffset,
            final long endingOffset) {
        observer.restoreStarted(
                topicPartition.topic(),
                topicPartition.partition(),
                storeName,
                startingOffset,
                endingOffset);
    }

    @Override
    public void onBatchRestored(
            final TopicPartition topicPartition,
            final String storeName,
            final long batchEndOffset,
            final long restored) {}

    @Override
    public void onRestoreEnd(
            final TopicPartition topicPartition, final String storeName, final long totalRestored) {
        observer.restoreFinished(
                topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
    }
}
