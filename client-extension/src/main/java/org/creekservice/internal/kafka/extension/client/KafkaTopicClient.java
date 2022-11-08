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

package org.creekservice.internal.kafka.extension.client;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.client.TopicClient;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.observability.logging.structured.LogEntryCustomizer;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;

/** Implementation of {@link TopicClient} */
public final class KafkaTopicClient implements TopicClient {

    private final StructuredLogger logger;
    private final ClustersProperties clusterProps;
    private final Function<Map<String, Object>, Admin> adminFactory;

    /** @param clusterProps props */
    public KafkaTopicClient(final ClustersProperties clusterProps) {
        this(
                clusterProps,
                Admin::create,
                StructuredLoggerFactory.internalLogger(KafkaTopicClient.class));
    }

    @VisibleForTesting
    KafkaTopicClient(
            final ClustersProperties clusterProps,
            final Function<Map<String, Object>, Admin> adminFactory,
            final StructuredLogger logger) {
        this.clusterProps = requireNonNull(clusterProps, "clusterProps");
        this.adminFactory = requireNonNull(adminFactory, "adminFactory");
        this.logger = requireNonNull(logger, "logger");
    }

    public void ensure(final List<? extends CreatableKafkaTopic<?, ?>> topics) {
        final Map<String, List<CreatableKafkaTopic<?, ?>>> byCluster =
                topics.stream().collect(groupingBy(CreatableKafkaTopic::cluster));

        byCluster.forEach(this::ensure);
    }

    private void ensure(final String cluster, final List<CreatableKafkaTopic<?, ?>> topics) {
        logger.info(
                "Ensuring topics",
                log ->
                        log.with(
                                "topic-ids",
                                topics.stream().map(CreatableKafkaTopic::id).collect(toList())));

        try (Admin admin = adminFactory.apply(clusterProps.get(cluster))) {
            create(topics, cluster, admin);
        }
    }

    /**
     * Try and create all topics, as any "check first, then create" approach inherently has race
     * conditions, especially considering its common to have several instances of a service starting
     * at once.
     */
    private void create(
            final List<CreatableKafkaTopic<?, ?>> topics, final String cluster, final Admin admin) {

        final List<NewTopic> newTopics =
                topics.stream().map(KafkaTopicClient::toNewTopic).collect(toList());

        final CreateTopicsResult result = admin.createTopics(newTopics);

        final Consumer<Map.Entry<String, KafkaFuture<Void>>> throwOnFailure =
                e -> {
                    final String topic = e.getKey();
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
                                            log.with("cluster", cluster)
                                                    .with("name", topic)
                                                    .with("partitions", partitions)
                                                    .ns("config");

                                    config.forEach(c -> configNs.with(c.name(), c.value()));
                                });
                    } catch (ExecutionException ex) {
                        if (!(ex.getCause() instanceof TopicExistsException)) {
                            throw new CreateTopicException(topic, cluster, ex.getCause());
                        }
                        logger.debug("Topic already exists", log -> log.with("nane", topic));
                    } catch (Exception ex) {
                        throw new CreateTopicException(topic, cluster, ex);
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
        CreateTopicException(
                final String topicName, final String clusterName, final Throwable cause) {
            super(
                    "Failed to create topic. topic: " + topicName + ", cluster: " + clusterName,
                    cause);
        }
    }
}
