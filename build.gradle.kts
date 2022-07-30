import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.jetaudit"
version = "1.1.9-SNAPSHOT"

plugins {
    kotlin("jvm") version "1.5.32" apply true
    id("tanvd.kosogor") version "1.0.12" apply true
}

val artifactoryUploadEnabled = System.getenv("artifactory_url") != null

repositories {
    mavenCentral()
    if (artifactoryUploadEnabled)
        maven(System.getenv("artifactory_url")!!)

    System.getenv("aorm_repo_url")?.let { aorm_repo ->
        maven(aorm_repo)
    } ?: maven("https://jitpack.io")
}

dependencies {
    api("org.slf4j", "slf4j-api", "1.7.30")

    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    api("tanvd.aorm", "aorm", "1.1.11")
    api("com.amazonaws", "aws-java-sdk-s3", "1.12.55")

    testImplementation("ch.qos.logback", "logback-classic", "1.2.2")

    testImplementation("junit", "junit", "4.12")
    testImplementation("org.testcontainers", "testcontainers", "1.17.3")
    testImplementation("org.testcontainers", "clickhouse", "1.17.3")

    testImplementation("org.mockito", "mockito-core", "3.12.4")
    testImplementation("org.powermock", "powermock-api-mockito2", "2.0.9")
    testImplementation("org.powermock", "powermock-core", "2.0.9")
    testImplementation("org.powermock", "powermock-module-junit4", "2.0.9")
}

tasks.withType(JavaCompile::class) {
    targetCompatibility = "11"
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "11"
        languageVersion = "1.5"
        apiVersion = "1.5"
    }
}

tasks.withType<Test> {
    useJUnit()

    testLogging {
        events("passed", "skipped", "failed")
    }
}

publishJar {
    publication {
        artifactId = "jetaudit"
    }


    if (artifactoryUploadEnabled) {
        artifactory {
            serverUrl = System.getenv("artifactory_url")
            repository = System.getenv("artifactory_repo")
            username = System.getenv("artifactory_username")
            secretKey = System.getenv("artifactory_api_key") ?: ""
        }
    }
}
