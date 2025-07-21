/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Objects;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@GeneratesSchema
public final class TypeWithImplicitPolymorphism {

    private final Inner inner;

    public TypeWithImplicitPolymorphism(@JsonProperty("inner") final Inner inner) {
        this.inner = requireNonNull(inner, "inner");
    }

    @JsonGetter("inner")
    public Inner inner() {
        return inner;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TypeWithImplicitPolymorphism testValueV1 = (TypeWithImplicitPolymorphism) o;
        return Objects.equals(inner, testValueV1.inner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inner);
    }

    @Override
    public String toString() {
        return "TestValueWithEnum{" + "inner=" + inner + '}';
    }

    // Poly type with no explicit subtype info.
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    private interface Inner {}

    // With explicit logical name:
    @JsonTypeName("the-explicit-name")
    public static final class ExplicitlyNamed implements Inner {

        private final String text;

        public ExplicitlyNamed(@JsonProperty("text") final String text) {
            this.text = text;
        }

        @SuppressWarnings("unused")
        public String getText() {
            return text;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ExplicitlyNamed that = (ExplicitlyNamed) o;
            return Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text);
        }

        @Override
        public String toString() {
            return "InnerTypeA{" + "text='" + text + '\'' + '}';
        }
    }

    // Without explicit logical name:
    public static final class ImplicitlyNamed implements Inner {
        private final int age;

        public ImplicitlyNamed(@JsonProperty("age") final int age) {
            this.age = age;
        }

        @SuppressWarnings("unused")
        public int getAge() {
            return age;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ImplicitlyNamed that = (ImplicitlyNamed) o;
            return age == that.age;
        }

        @Override
        public int hashCode() {
            return Objects.hash(age);
        }

        @Override
        public String toString() {
            return "InnerTypeB{" + "age='" + age + '\'' + '}';
        }
    }
}
