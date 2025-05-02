import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import tanvd.kosogor.proxy.publishJar

group = "tanvd.jetaudit"
version = "1.2.2"

plugins {
    kotlin("jvm") version "2.1.20" apply true
    id("tanvd.kosogor") version "1.0.18" apply true
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
    api("org.slf4j", "slf4j-api", "2.0.17")

    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    api("tanvd.aorm", "aorm", "1.1.19")

    api("software.amazon.awssdk", "s3", "2.31.1")

    testImplementation("ch.qos.logback", "logback-classic", "1.4.7")

    testImplementation("junit", "junit", "4.12")
    testImplementation("org.testcontainers", "clickhouse", "1.21.0")
    testImplementation("org.lz4", "lz4-java", "1.8.0")

    testImplementation("org.mockito", "mockito-core", "3.12.4")
    testImplementation("org.powermock", "powermock-api-mockito2", "2.0.9")
    testImplementation("org.powermock", "powermock-core", "2.0.9")
    testImplementation("org.powermock", "powermock-module-junit4", "2.0.9")
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        apiVersion.set(KotlinVersion.KOTLIN_2_1)
        languageVersion.set(KotlinVersion.KOTLIN_2_1)
        // https://jakewharton.com/kotlins-jdk-release-compatibility-flag/
        // https://youtrack.jetbrains.com/issue/KT-49746/Support-Xjdk-release-in-gradle-toolchain#focus=Comments-27-8935065.0-0
        freeCompilerArgs.addAll("-Xjdk-release=17")
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of("17"))
    }
}

tasks.withType<Test> {
    jvmArgs = listOf(
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--add-opens", "java.base/java.io=ALL-UNNAMED"
    )

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
