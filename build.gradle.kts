import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.jetaudit"
version = "1.1.4-SNAPSHOT"

plugins {
    kotlin("jvm") version "1.3.31" apply true
    id("tanvd.kosogor") version "1.0.5" apply true
}

repositories {
    jcenter()
}

dependencies {
    compile("org.slf4j", "slf4j-api", "1.7.25")

    compile(kotlin("stdlib"))
    compile(kotlin("reflect"))

    compile("tanvd.aorm", "aorm", "1.1.4")
    compile("com.amazonaws", "aws-java-sdk-s3", "1.11.546")

    testCompile("ch.qos.logback", "logback-classic", "1.2.2")

    testCompile("junit", "junit", "4.12")
    testCompile("org.testcontainers", "testcontainers", "1.11.2")

    testCompile("org.mockito", "mockito-core", "1.10.19")
    testCompile("org.powermock", "powermock-api-mockito", "1.6.4")
    testCompile("org.powermock", "powermock-core", "1.6.4")
    testCompile("org.powermock", "powermock-module-junit4", "1.6.4")
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
        languageVersion = "1.3"
        apiVersion = "1.3"
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
