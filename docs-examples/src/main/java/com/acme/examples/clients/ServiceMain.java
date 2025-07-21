/*
 * Copyright 2025 Creek Contributors (https://github.com/creek-service)
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

package com.acme.examples.clients;

import com.acme.examples.service.MyServiceDescriptor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;

import java.time.Duration;
import java.util.List;

// begin-snippet: service-main
public final class ServiceMain {

    public static void main(String... args) {
        // Initialize Creek in the main application entry point:
        CreekContext ctx = CreekServices.context(new MyServiceDescriptor());

        // Access the extension, and use to help create the application:
        new ServiceMain(ctx.extension(KafkaClientsExtension.class)).run();
    }

    private final KafkaClientsExtension ext;
    private final KafkaTopic<Long, String> inputTopic;
    private final KafkaTopic<Long, String> outputTopic;

    private ServiceMain(KafkaClientsExtension ext) {
        this.ext = ext;
        // Retrieve type-safe topic metadata and serde:
        this.inputTopic = ext.topic(MyServiceDescriptor.InputTopic);
        this.outputTopic = ext.topic(MyServiceDescriptor.OutputTopic);
    }

    private void run() {
        // Acquire Kafka consumers and producers:
        try (Consumer<byte[], byte[]> consumer = ext.consumer();
             Producer<byte[], byte[]> producer = ext.producer()) {

            consumer.subscribe(List.of(inputTopic.name()));

            while (running()) {
                consumer.poll(Duration.ofSeconds(1))
                        .records(inputTopic.name())
                        .forEach(r -> processInput(r, producer));
            }
        }
    }

    private void processInput(final ConsumerRecord<byte[], byte[]> input,
                              final Producer<byte[], byte[]> producer) {

        // Access type-safe topic deserializers:
        long key = inputTopic.deserializeKey(input.key());
        String value = inputTopic.deserializeValue(input.value());

        // ... do stuff with key & value.

        producer.send(new ProducerRecord<>(outputTopic.name(),
                // Access type-safe topic serializers:
                outputTopic.serializeKey(key),
                outputTopic.serializeValue(value)
        ));
    }
    // end-snippet

    private boolean running() {
        return false;
    }

    public static void extensionOptions(String... args) {
        // begin-snippet: extension-options
        CreekContext ctx = CreekServices.builder(new MyServiceDescriptor())
                .with(
                        KafkaClientsExtensionOptions.builder()
                                .withKafkaProperty(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, 200L)
                                .build()
                )
                .build();
        // end-snippet
        new ServiceMain(ctx.extension(KafkaClientsExtension.class)).run();
    }
}
