/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension.client;

import static java.lang.System.lineSeparator;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.client.TopicClient;
import org.creekservice.api.kafka.extension.logging.LoggingField;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.observability.logging.structured.LogEntryCustomizer;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;

/**
 * Implementation of {@link TopicClient}.
 *
 * <p>Responsible for ensuring topics are created, along with any associated resources, e.g.
 * schemas.
 */
public final class KafkaTopicClient implements TopicClient {

    private final StructuredLogger logger;
    private final String clusterName;
    private final Map<String, Object> kafkaProperties;
    private final Function<Map<String, Object>, Admin> adminFactory;

    /**
     * @param clusterName the name of the cluster this client connects to.
     * @param kafkaProperties properties for connecting to the cluster.
     */
    public KafkaTopicClient(final String clusterName, final Map<String, Object> kafkaProperties) {
        this(
                clusterName,
                kafkaProperties,
                Admin::create,
                StructuredLoggerFactory.internalLogger(KafkaTopicClient.class));
    }

    @VisibleForTesting
    KafkaTopicClient(
            final String clusterName,
            final Map<String, Object> kafkaProperties,
            final Function<Map<String, Object>, Admin> adminFactory,
            final StructuredLogger logger) {
        this.clusterName = requireNonNull(clusterName, "clusterName");
        this.kafkaProperties = requireNonNull(kafkaProperties, "kafkaProperties");
        this.adminFactory = requireNonNull(adminFactory, "adminFactory");
        this.logger = requireNonNull(logger, "logger");
    }

    public void ensureTopicsExist(final List<? extends CreatableKafkaTopic<?, ?>> topics) {
        validateCluster(topics);

        try (Admin admin = adminFactory.apply(kafkaProperties)) {
            create(topics, admin);
        }
    }

    private void validateCluster(final List<? extends CreatableKafkaTopic<?, ?>> topics) {
        final List<URI> wrongCluster =
                topics.stream()
                        .filter(topic -> !topic.cluster().equals(clusterName))
                        .map(CreatableKafkaTopic::id)
                        .collect(Collectors.toList());

        if (!wrongCluster.isEmpty()) {
            throw new IllegalArgumentException(
                    "topics were for wrong cluster."
                            + lineSeparator()
                            + "Expected cluster: "
                            + clusterName
                            + lineSeparator()
                            + "Invalid topic ids: "
                            + wrongCluster);
        }
    }

    /**
     * Try and create all topics, as any "check first, then create" approach inherently has race
     * conditions, especially considering its common to have several instances of a service starting
     * at once.
     */
    private void create(final List<? extends CreatableKafkaTopic<?, ?>> topics, final Admin admin) {

        final List<NewTopic> newTopics =
                topics.stream().map(KafkaTopicClient::toNewTopic).collect(toList());

        final CreateTopicsResult result = admin.createTopics(newTopics);

        final Consumer<Map.Entry<String, KafkaFuture<Void>>> throwOnFailure =
                e -> {
                    final String topic = e.getKey();
                    final URI topicId = KafkaTopicDescriptor.resourceId(clusterName, topic);
                    try {
                        e.getValue().get();

                        final Integer partitions = result.numPartitions(topic).get();
                        final List<ConfigEntry> config =
                                result.config(topic).get().entries().stream()
                                        .filter(c -> c.source() == DYNAMIC_TOPIC_CONFIG)
                                        .collect(toList());

                        logger.info(
                                "Created topic",
                                log -> {
                                    final LogEntryCustomizer configNs =
                                            log.with(LoggingField.topicId, topicId)
                                                    .with(LoggingField.partitions, partitions)
                                                    .ns("config");

                                    config.forEach(c -> configNs.with(c.name(), c.value()));
                                });
                    } catch (ExecutionException ex) {
                        if (!(ex.getCause() instanceof TopicExistsException)) {
                            throw new CreateTopicException(topicId, ex.getCause());
                        }
                        logger.debug(
                                "Topic already exists",
                                log -> log.with(LoggingField.topicId, topicId));
                    } catch (Exception ex) {
                        throw new CreateTopicException(topicId, ex);
                    }
                };

        result.values().entrySet().forEach(throwOnFailure);
    }

    private static NewTopic toNewTopic(final CreatableKafkaTopic<?, ?> descriptor) {
        return new NewTopic(
                        descriptor.name(),
                        Optional.of(descriptor.config().partitions()),
                        Optional.empty())
                .configs(descriptor.config().config());
    }

    private static final class CreateTopicException extends RuntimeException {
        CreateTopicException(final URI topicId, final Throwable cause) {
            super("Failed to create topic. " + LoggingField.topicId + ": " + topicId, cause);
        }
    }
}
