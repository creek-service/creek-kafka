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
val creekBaseVersion : String by extra
val creekServiceVersion : String by extra
val creekObsVersion : String by extra
val spotBugsVersion : String by extra

dependencies {
    api(project(":metadata"))
    api(project(":common"))
    api("org.creekservice:creek-base-annotation:$creekBaseVersion")
    api("org.creekservice:creek-service-extension:$creekServiceVersion")
    api("org.apache.kafka:kafka-streams:$kafkaVersion")

    implementation(project(":serde"))
    implementation("org.creekservice:creek-observability-logging:$creekObsVersion")
    implementation("org.creekservice:creek-base-type:$creekBaseVersion")
    implementation("com.github.spotbugs:spotbugs-annotations:$spotBugsVersion")

    testImplementation("org.creekservice:creek-observability-logging-fixtures:$creekObsVersion")
}
