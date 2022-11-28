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

import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import java.nio.file.Paths

plugins {
    application
    id("com.bmuschko.docker-remote-api") version "8.1.0"
}

val creekVersion : String by extra
val log4jVersion : String by extra

dependencies {
    implementation("org.creekservice:creek-service-context:$creekVersion")
    implementation(project(":streams-extension"))

    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:$log4jVersion")
}

application {
    mainModule.set("creek.kafka.test.service")
    mainClass.set("org.creekservice.internal.kafka.test.service.ServiceMain")
}

val buildAppImage = tasks.create("buildAppImage", DockerBuildImage::class) {
    dependsOn("prepareDocker")
    buildArgs.put("APP_NAME", project.name)
    buildArgs.put("APP_VERSION", "${project.version}")
    images.add("ghcr.io/creek-service/${rootProject.name}-${project.name}:latest")
}

tasks.register<Copy>("prepareDocker") {
    dependsOn("distTar")

    from(
        layout.projectDirectory.file("Dockerfile"),
        layout.buildDirectory.file("distributions/${project.name}-${project.version}.tar"),
        layout.projectDirectory.dir("include"),
    )

    // Include the AttachMe agent files if present in user's home directory:
    from (Paths.get(System.getProperty("user.home")).resolve(".attachme")) {
        into("agent")
    }

    // Ensure the agent dir exists even if the agent is not installed
    from (layout.projectDirectory.file(".ensureAgent")) {
        into("agent")
    }

    into(buildAppImage.inputDir)
}