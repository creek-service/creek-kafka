/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.serde.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.lang.module.ModuleDescriptor;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;

/**
 * Helper for testing a serde provider is valid.
 *
 * <p>Will test the serde provider is correctly registered both for the class-path, and for use on
 * the module-path.
 *
 * <p>Usage:
 *
 * <pre>
 * KafkaSerdeProviderTester.tester(MySerdeProvider.class)
 *   .withExpectedFormat(serializationFormat("foo"))
 *   .test()
 * </pre>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class KafkaSerdeProviderTester {

    private final Class<? extends KafkaSerdeProvider> providerType;
    private final Supplier<Stream<? extends KafkaSerdeProvider>> serviceLoader;
    private Optional<Check> classPathCheck;
    private Optional<Check> modulePathCheck;
    private Optional<Check> formatCheck = Optional.empty();

    @VisibleForTesting
    KafkaSerdeProviderTester(
            final Class<? extends KafkaSerdeProvider> providerType,
            final Supplier<Stream<? extends KafkaSerdeProvider>> serviceLoader) {
        this.providerType = requireNonNull(providerType, "providerType");
        this.serviceLoader = requireNonNull(serviceLoader, "serviceLoader");
        this.classPathCheck = Optional.of(() -> validateRegisteredOnClassPath(providerType));
        this.modulePathCheck = Optional.of(() -> validateRegisteredOnModulePath(providerType));
    }

    /**
     * Create a tester for a specific type.
     *
     * @param providerType the provider type.
     * @return the tester.
     */
    public static KafkaSerdeProviderTester tester(
            final Class<? extends KafkaSerdeProvider> providerType) {
        return new KafkaSerdeProviderTester(
                providerType,
                () ->
                        ServiceLoader.load(KafkaSerdeProvider.class).stream()
                                .map(ServiceLoader.Provider::get));
    }

    /**
     * Disable testing serde is registered in {@code META-INF/services}.
     *
     * @return self.
     */
    public KafkaSerdeProviderTester withoutTestingClassPath() {
        this.classPathCheck = Optional.empty();
        return this;
    }

    /**
     * Disable testing serde is registered {@code module-info.java}.
     *
     * <p>**Note**: if the serde provider is not loaded from the module path, or does not define a
     * {@code module-info.java}, this is auto-disabled.
     *
     * @return self.
     */
    public KafkaSerdeProviderTester withoutTestingModulePath() {
        this.modulePathCheck = Optional.empty();
        return this;
    }

    /**
     * Check the providers serialization format is the expected value.
     *
     * @param format expected format
     * @return self.
     */
    public KafkaSerdeProviderTester withExpectedFormat(final SerializationFormat format) {
        Objects.requireNonNull(format, "format");
        this.formatCheck = Optional.of(() -> validateFormat(format));
        return this;
    }

    /** Run the configured checks */
    public void test() {
        classPathCheck.ifPresent(Check::check);
        modulePathCheck.ifPresent(Check::check);
        formatCheck.ifPresent(Check::check);
    }

    private static void validateRegisteredOnClassPath(
            final Class<? extends KafkaSerdeProvider> providerType) {
        final boolean notRegistered =
                providerType
                        .getClassLoader()
                        .resources(
                                "META-INF/services/org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider")
                        .flatMap(servicesFile -> extractProviders(providerType, servicesFile))
                        .noneMatch(type -> type.equals(providerType.getName()));

        if (notRegistered) {
            throw new KafkaSerdeProviderValidationFailure(
                    providerType,
                    "Provider not registered in META-INF/services. See"
                        + " https://www.creekservice.org/creek-kafka/#formats-on-the-class-path");
        }
    }

    @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD", justification = "URL not user supplied")
    private static Stream<String> extractProviders(
            final Class<? extends KafkaSerdeProvider> providerType1, final URL servicesFile) {
        try (InputStream is = servicesFile.openStream()) {
            final String content = new String(is.readAllBytes(), UTF_8);
            return Arrays.stream(content.split(System.lineSeparator()))
                    .map(String::trim)
                    .filter(s -> !s.startsWith("#"))
                    .filter(s -> !s.isBlank());
        } catch (IOException e) {
            throw new KafkaSerdeProviderValidationFailure(
                    providerType1, "Failed to open services file: " + servicesFile);
        }
    }

    private static void validateRegisteredOnModulePath(
            final Class<? extends KafkaSerdeProvider> providerType) {
        final Module module = providerType.getModule();
        if (!module.isNamed() || module.getDescriptor().isAutomatic()) {
            // not on module path or not module:
            return;
        }

        final boolean notRegistered =
                module.getDescriptor().provides().stream()
                        .filter(
                                provides ->
                                        provides.service()
                                                .equals(KafkaSerdeProvider.class.getName()))
                        .map(ModuleDescriptor.Provides::providers)
                        .flatMap(Collection::stream)
                        .noneMatch(type -> type.equals(providerType.getName()));

        if (notRegistered) {
            throw new KafkaSerdeProviderValidationFailure(
                    providerType,
                    "Provider not registered in module descriptor. See"
                        + " https://www.creekservice.org/creek-kafka/#formats-on-the-module-path");
        }
    }

    private void validateFormat(final SerializationFormat expected) {
        final KafkaSerdeProvider provider = load(providerType);

        final SerializationFormat actual = provider.format();
        if (actual == null) {
            throw new KafkaSerdeProviderValidationFailure(providerType, "format is null");
        }

        if (!expected.equals(actual)) {
            throw new KafkaSerdeProviderValidationFailure(
                    providerType, "unexpected format", expected, actual);
        }
    }

    private KafkaSerdeProvider load(final Class<? extends KafkaSerdeProvider> providerType) {
        return serviceLoader
                .get()
                .filter(p -> p.getClass().equals(providerType))
                .findAny()
                .orElseThrow(
                        () ->
                                new KafkaSerdeProviderValidationFailure(
                                        providerType,
                                        "Provider not registered. See"
                                            + " https://www.creekservice.org/creek-kafka/#registering-custom-formats"));
    }

    @FunctionalInterface
    private interface Check {
        void check();
    }

    private static class KafkaSerdeProviderValidationFailure extends AssertionError {

        KafkaSerdeProviderValidationFailure(
                final Class<? extends KafkaSerdeProvider> providerType, final String msg) {
            super(providerType.getName() + ": " + msg);
        }

        <T> KafkaSerdeProviderValidationFailure(
                final Class<? extends KafkaSerdeProvider> providerType,
                final String msg,
                final T expected,
                final T actual) {
            super(
                    providerType.getName()
                            + ": "
                            + msg
                            + ". expected: "
                            + expected
                            + ", actual: "
                            + actual);
        }
    }
}
