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

package org.creekservice.internal.kafka.streams.extension.config;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.platform.metadata.ComponentDescriptor;

public final class ClustersPropertiesFactory {

    private final TopicCollector topicCollector;

    public ClustersPropertiesFactory() {
        this(org.creekservice.internal.kafka.common.resource.TopicCollector::collectTopics);
    }

    @VisibleForTesting
    ClustersPropertiesFactory(final TopicCollector topicCollector) {
        this.topicCollector = requireNonNull(topicCollector, "topicCollector");
    }

    public ClustersProperties create(
            final Collection<? extends ComponentDescriptor> components,
            final KafkaStreamsExtensionOptions apiOptions) {

        final Set<String> clusterNames =
                topicCollector.collectTopics(components).values().stream()
                        .map(KafkaTopicDescriptor::cluster)
                        .collect(Collectors.toSet());

        final ClustersProperties overrides = apiOptions.propertyOverrides().get(clusterNames);
        return apiOptions.propertiesBuilder().putAll(overrides).build();
    }

    @VisibleForTesting
    interface TopicCollector {
        Map<URI, KafkaTopicDescriptor<?, ?>> collectTopics(
                Collection<? extends ComponentDescriptor> components);
    }
}
