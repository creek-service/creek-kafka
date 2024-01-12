/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

plugins {
    application
    id("com.bmuschko.docker-remote-api")
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
    mainModule.set("creek.kafka.test.service.inbuilt")
    mainClass.set("org.creekservice.internal.kafka.test.service.inbuilt.ServiceMain")
}

val buildAppImage = tasks.create("buildAppImage", DockerBuildImage::class) {
    dependsOn("prepareDocker")
    buildArgs.put("APP_NAME", project.name)
    buildArgs.put("APP_VERSION", "${project.version}")
    images.add("ghcr.io/creek-service/${rootProject.name}-${project.name}:latest")

    onlyIf {
        // Exclude the task if running on Windows (as images don't build on Windows)
        !System.getProperty("os.name").lowercase().contains("win")
    }
}

tasks.register<Copy>("prepareDocker") {
    dependsOn("distTar")

    from(
        layout.projectDirectory.file("Dockerfile"),
        tarTree(layout.buildDirectory.file("distributions/${project.name}-${project.version}.tar")),
        layout.projectDirectory.dir("include"),
    )

    into(buildAppImage.inputDir)
}