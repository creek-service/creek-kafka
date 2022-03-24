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

plugins {
    `java-library`
}

val kafkaVersion : String by extra
val creekVersion : String by extra


dependencies {
    api(project(":metadata"))
    api("org.creek:creek-service-context:$creekVersion")
    api("org.creek:creek-test-util:$creekVersion")
    api("org.apache.kafka:kafka-streams-test-utils:${kafkaVersion}")

    implementation("org.creek:creek-kafka-streams-extension:$creekVersion")
}
