import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.jetaudit"
version = "1.1.5-SNAPSHOT"

plugins {
    kotlin("jvm") version "1.4.0" apply true
    id("tanvd.kosogor") version "1.0.10" apply true
}

repositories {
    jcenter()
}

dependencies {
    api("org.slf4j", "slf4j-api", "1.7.25")

    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    api("tanvd.aorm", "aorm", "1.1.6")
    api("com.amazonaws", "aws-java-sdk-s3", "1.11.845")

    testImplementation("ch.qos.logback", "logback-classic", "1.2.2")

    testImplementation("junit", "junit", "4.12")
    testImplementation("org.testcontainers", "testcontainers", "1.14.3")

    testImplementation("org.mockito", "mockito-core", "1.10.19")
    testImplementation("org.powermock", "powermock-api-mockito", "1.6.4")
    testImplementation("org.powermock", "powermock-core", "1.6.4")
    testImplementation("org.powermock", "powermock-module-junit4", "1.6.4")
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
        languageVersion = "1.4"
        apiVersion = "1.4"
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

    bintray {
        username = "tanvd"
        repository = "jetaudit"
        info {
            githubRepo = "tanvd/jetaudit"
            vcsUrl = "https://github.com/tanvd/jetaudit"
            labels.addAll(listOf("kotlin", "clickhouse", "audit"))
            license = "MIT"
            description = "Kotlin library for a business audit upon Clickhouse"
        }
    }
}
