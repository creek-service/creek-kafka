package com.acme.examples.client;

import com.acme.examples.service.MyServiceDescriptor;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.client.TopicClient;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

@SuppressWarnings("FieldCanBeLocal")
// begin-snippet: all
class CustomClientTest {

    private static CreekContext ctx;
    private static KafkaClientsExtension ext;

    @BeforeAll
    public static void classSetup() {
        ctx = CreekServices.builder(new MyServiceDescriptor())
                // Configure Creek to work without an actual cluster:
                .with(KafkaClientsExtensionOptions.builder()
                        .withTypeOverride(TopicClient.Factory.class, CustomTopicClient::new)
                        .build())
                .build();

        ext = ctx.extension(KafkaClientsExtension.class);
    }

    @AfterAll
    static void afterAll() {
        ctx.close();
    }

    // Tests are free to get serde from ext...
}
// end-snippet