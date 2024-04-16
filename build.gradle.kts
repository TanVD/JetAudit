import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile
import tanvd.kosogor.proxy.publishJar

group = "tanvd.jetaudit"
version = "1.2.0"

plugins {
    kotlin("jvm") version "1.9.22" apply true
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
    api("org.slf4j", "slf4j-api", "1.7.36")

    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    api("tanvd.aorm", "aorm", "1.1.16")

    api("software.amazon.awssdk", "s3", "2.25.14")

    testImplementation("ch.qos.logback", "logback-classic", "1.4.7")

    testImplementation("junit", "junit", "4.12")
    testImplementation("org.testcontainers", "clickhouse", "1.18.1")
    testImplementation("org.lz4", "lz4-java", "1.8.0")

    testImplementation("org.mockito", "mockito-core", "3.12.4")
    testImplementation("org.powermock", "powermock-api-mockito2", "2.0.9")
    testImplementation("org.powermock", "powermock-core", "2.0.9")
    testImplementation("org.powermock", "powermock-module-junit4", "2.0.9")
}

tasks.withType(JavaCompile::class) {
    targetCompatibility = "11"
    sourceCompatibility = "11"
}

tasks.withType<KotlinJvmCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "11"
        apiVersion = "1.9"
        languageVersion = "1.9"
        freeCompilerArgs += "-Xuse-ir"
        freeCompilerArgs += "-Xbackend-threads=3"
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
