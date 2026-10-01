/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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
package org.creekservice.api.kafka.test.service.inbuilt;

import static java.util.Objects.requireNonNull;

import java.util.List;
import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.ComponentOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;

/** A pipeline fixture with one String/String input, one String/String output, and a stage. */
public abstract class PipelineServiceDescriptor implements ServiceDescriptor {
    private final List<ComponentInput> inputs;
    private final List<ComponentOutput> outputs;
    private final String stage;

    protected PipelineServiceDescriptor(
            final ComponentInput input, final ComponentOutput output, final String stage) {
        this.inputs = List.of(requireNonNull(input, "input"));
        this.outputs = List.of(requireNonNull(output, "output"));
        this.stage = requireNonNull(stage, "stage");
    }

    /** The suffix appended to each value processed by this service. */
    public final String stage() {
        return stage;
    }

    @Override
    public final String dockerImage() {
        return "ghcr.io/creek-service/creek-kafka-test-service-native";
    }

    @Override
    public final List<ComponentInput> inputs() {
        return inputs;
    }

    @Override
    public final List<ComponentOutput> outputs() {
        return outputs;
    }
}
