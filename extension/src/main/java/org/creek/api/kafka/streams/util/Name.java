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

package org.creek.api.kafka.streams.util;

import static java.util.Objects.requireNonNull;

import org.apache.kafka.streams.kstream.Named;

/**
 * Helper class for building unique node names in a Kafka Streams topology.
 *
 * <p>This class can help to ensure unique node names when the building of a topology
 * is spread across multiple methods or classes.
 *
 * <p>Create a single top level {@code Name} instance. Use {@link Name#postfix postfixes}
 * when passing the {@code Name} instance to methods that build part of the topology. Use
 * {@link Name#name} and {@code Name#named} when creating a node name. For example:
 *
 * <pre>{@code
 * class TopologyBuilder {
 *
 *     private static final Name NAME = Name.root();
 *
 *     Topology build() {
 *        StreamBuilder builder = new StreamBuilder();
 *
 *        final KStream<Long, String> things =
 *                 builder.stream(
 *                                 ThingsTopic.topicName(),
 *                                 Consumed.with(
 *                                                 ctx.keySerdeFor(ThingsTopic),
 *                                                 ctx.valueSerdeFor(ThingsTopic))
 *                                         .withName(name.name("ingest")))
 *                         .transformValues(
 *                                 new ThingTransformer(), name.named("transform"));
 *
 *        new SubTopologyOneBuilder().build(NAME.postfix("sub1"), builder);
 *        new SubTopologyTwoBuilder().build(NAME.postfix("sub2"), builder);
 *
 *        return builder.build();
 *     }
 * }</pre>
 */
public final class Name {

    public static final char DEFAULT_DELIM = '.';

    private final char delim;
    private final String prefix;

    public static Name root() {
        return root(DEFAULT_DELIM);
    }

    public static Name root(final char delimiter) {
        return new Name("", delimiter);
    }

    private Name(final String prefix, final char delim) {
        this.delim = delim;
        this.prefix = requireNonNull(prefix, "prefix") + (prefix.isEmpty() ? "" : delim);
    }

    public String name(final String postfix) {
        return prefix + postfix;
    }

    public Named named(final String postfix) {
        return Named.as(name(postfix));
    }

    public Name postfix(final String postfix) {
        return new Name(name(postfix), delim);
    }
}
