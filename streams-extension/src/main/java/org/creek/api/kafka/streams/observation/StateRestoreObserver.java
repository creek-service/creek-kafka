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

package org.creek.api.kafka.streams.observation;

public interface StateRestoreObserver {

    /**
     * Method called at the beginning of state store's restoration.
     *
     * @param topic the topic containing the values to restore
     * @param partition the partition containing the values to restore
     * @param storeName the name of the store undergoing restoration
     * @param startingOffset the starting offset of the entire restoration process for this
     *     TopicPartition
     * @param endingOffset the exclusive ending offset of the entire restoration process for this
     *     TopicPartition
     */
    default void restoreStarted(
            final String topic,
            final int partition,
            final String storeName,
            final long startingOffset,
            final long endingOffset) {}

    /**
     * Method called when restoring a state store is complete.
     *
     * @param topic the topic containing the values to restore
     * @param partition the partition containing the values to restore
     * @param storeName the name of the store just restored
     * @param totalRestored the total number of records restored for this TopicPartition
     */
    default void restoreFinished(
            final String topic,
            final int partition,
            final String storeName,
            final long totalRestored) {}
}
