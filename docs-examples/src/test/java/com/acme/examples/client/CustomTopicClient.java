package com.acme.examples.client;

import org.creekservice.api.kafka.extension.client.TopicClient;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;

import java.util.List;
import java.util.Map;

public class CustomTopicClient implements  TopicClient {
    public CustomTopicClient(final String cluster, final Map<String, Object> properties) {

    }

    @Override
    public void ensureTopicsExist(final List<? extends CreatableKafkaTopic<?, ?>> topics) {

    }
}
