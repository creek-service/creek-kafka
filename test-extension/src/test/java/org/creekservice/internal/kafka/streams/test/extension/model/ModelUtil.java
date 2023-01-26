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

package org.creekservice.internal.kafka.streams.test.extension.model;

import static org.mockito.Mockito.mock;

import org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester;
import org.creekservice.api.system.test.test.util.ModelParser;
import org.creekservice.internal.kafka.streams.test.extension.KafkaTestExtension;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicExpectationHandler;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicInputHandler;

final class ModelUtil {

    private ModelUtil() {}

    static ModelParser createParser() {
        final CreekSystemTestExtensionTester.YamlParserBuilder builder =
                CreekSystemTestExtensionTester.yamlParser();
        KafkaTestExtension.initializeModel(
                builder.model(),
                mock(TopicInputHandler.class),
                mock(TopicExpectationHandler.class));
        return builder.build();
    }
}
