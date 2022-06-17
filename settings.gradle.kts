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
    "streams-extension",
    "streams-test-extension",
    "streams-test",
    "serde",
    "common",
    "test-java-eight",
    "test-java-nine",
    "test-serde",
    "test-system-test",
    "hack" // Todo: rremove
)
