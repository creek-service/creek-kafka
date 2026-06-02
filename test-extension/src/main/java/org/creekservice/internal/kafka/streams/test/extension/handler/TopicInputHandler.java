/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.system.test.extension.test.model.InputHandler;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicInput;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;

/** {@link InputHandler} for {@link TopicInput}. */
public final class TopicInputHandler implements InputHandler<TopicInput> {

    private final ClientsExtension clientsExt;
    private final SystemTestSerdeProviders testSerdeProviders;
    private final Set<Producer<byte[], byte[]>> toFlush = new HashSet<>();
    private final TopicValidator topicValidator;

    /**
     * @param clientsExt the client extension.
     * @param testSerdeProviders the system test serde providers.
     * @param topicValidator a topic validator.
     */
    public TopicInputHandler(
            final ClientsExtension clientsExt,
            final SystemTestSerdeProviders testSerdeProviders,
            final TopicValidator topicValidator) {
        this.clientsExt = requireNonNull(clientsExt, "clientsExt");
        this.testSerdeProviders = requireNonNull(testSerdeProviders, "testSerdeProviders");
        this.topicValidator = requireNonNull(topicValidator, "topicValidator");
    }

    @Override
    public void process(final TopicInput input, final InputOptions options) {
        input.records().forEach(this::process);
    }

    @Override
    public void flush() {
        toFlush.forEach(Producer::flush);
        toFlush.clear();
    }

    private void process(final TopicRecord record) {
        final TestKafkaTopic testTopic = kafkaTopic(record);

        topicValidator.validateCanProduce(testTopic);

        final ProducerRecord<byte[], byte[]> producerRecord = serialize(record, testTopic);
        final Producer<byte[], byte[]> producer = clientsExt.producer(record.clusterName());
        producer.send(producerRecord);
        toFlush.add(producer);
    }

    private TestKafkaTopic kafkaTopic(final TopicRecord record) {
        try {
            final KafkaTopic<?, ?> topic =
                    clientsExt.topic(record.clusterName(), record.topicName());
            return testSerdeProviders.get(topic.descriptor());
        } catch (final Exception e) {
            throw new TopicInputException(
                    "The input record's cluster or topic is not known."
                            + " cluster: "
                            + record.clusterName()
                            + ", topic: "
                            + record.topicName()
                            + ", location: "
                            + record.location(),
                    e);
        }
    }

    private static ProducerRecord<byte[], byte[]> serialize(
            final TopicRecord record, final TestKafkaTopic testTopic) {
        boolean handlingKey = true;
        try {
            final byte[] key = record.key().map(testTopic::serializeKey).orElse(null, null);
            handlingKey = false;
            final byte[] value = record.value().map(testTopic::serializeValue).orElse(null, null);

            return new ProducerRecord<>(testTopic.name(), key, value);
        } catch (final Exception e) {
            final String part = handlingKey ? "key" : "value";
            final Object data = handlingKey ? record.key() : record.value();
            throw new TopicInputException(
                    "Failed to serialize the record's %s: %s, location: %s"
                            .formatted(part, data, record.location()),
                    e);
        }
    }

    private static final class TopicInputException extends RuntimeException {
        TopicInputException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }
}
