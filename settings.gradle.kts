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
    "test-extension",
    "test-java-eight",
    "test-java-nine",
    "test-serde",
    "test-service"
)
