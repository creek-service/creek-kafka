/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven {
            url = uri("https://maven.pkg.github.com/creek-service/*")
            credentials {
                username = "Creek-Bot-Token"
                password = "\u0067hp_LtyvXrQZen3WlKenUhv21Mg6NG38jn0AO2YH"
            }
        }
    }
}

rootProject.name = "creek-kafka"

include(
    "metadata",
    "client-extension",
    "streams-extension",
    "streams-test",
    "serde",
    "serde-test",
    "test-extension",
    "test-java-eight",
    "test-java-nine",
    "test-serde",
    "test-serde-java-eight",
    "test-service-native"
)
