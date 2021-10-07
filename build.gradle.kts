import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.jetaudit"
version = "1.1.7"

plugins {
    kotlin("jvm") version "1.5.31" apply true
    id("tanvd.kosogor") version "1.0.12" apply true
}

val artifactoryUploadEnabled = System.getenv("artifactory_url") != null

repositories {
    mavenCentral()
    if (artifactoryUploadEnabled)
        maven(System.getenv("artifactory_url")!!)

    System.getenv("aorm_repo_url")?.let { aorm_repo ->
        maven(aorm_repo)
    }
}

dependencies {
    api("org.slf4j", "slf4j-api", "1.7.30")

    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    api("tanvd.aorm", "aorm", "1.1.9")
    api("com.amazonaws", "aws-java-sdk-s3", "1.12.55")

    testImplementation("ch.qos.logback", "logback-classic", "1.2.2")

    testImplementation("junit", "junit", "4.12")
    testImplementation("org.testcontainers", "testcontainers", "1.16.0")
    testImplementation("org.testcontainers", "clickhouse", "1.16.0")

    testImplementation("org.mockito", "mockito-core", "1.10.19")
    testImplementation("org.powermock", "powermock-api-mockito", "1.6.4")
    testImplementation("org.powermock", "powermock-core", "1.6.4")
    testImplementation("org.powermock", "powermock-module-junit4", "1.6.4")
}

tasks.withType(JavaCompile::class) {
    targetCompatibility = "1.8"
}

tasks.withType<KotlinJvmCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
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
