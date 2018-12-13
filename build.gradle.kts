import com.jfrog.bintray.gradle.BintrayExtension
import groovy.lang.GroovyObject
import org.jfrog.gradle.plugin.artifactory.dsl.PublisherConfig

group = "tanvd.jetaudit"
version = "1.1.4-SNAPSHOT"

val kotlinVersion = "1.3.0"

plugins {
    kotlin("jvm") version "1.3.0" apply true
    `maven-publish` apply true
    id("com.jfrog.bintray") version "1.8.4" apply true
    id("com.jfrog.artifactory") version "4.7.5" apply true
}

repositories {
    jcenter()
    maven { setUrl("https://dl.bintray.com/jfrog/jfrog-jars") }
}

kotlin.sourceSets {
    this["main"].kotlin.also {
        it.srcDir("src/main")
    }
    this["test"].kotlin.also {
        it.srcDir("src/test")
    }
}

dependencies {
    compile("org.slf4j", "slf4j-api", "1.7.25")

    compile("org.jetbrains.kotlin", "kotlin-stdlib", kotlinVersion)
    compile("org.jetbrains.kotlin", "kotlin-reflect", kotlinVersion)

    compile("tanvd.aorm", "aorm", "1.1.2")
    compile("com.amazonaws", "aws-java-sdk-s3", "1.11.446")

    testCompile("ch.qos.logback", "logback-classic", "1.2.2")
    testCompile("org.testng", "testng", "6.11")
    testCompile("org.mockito", "mockito-all", "1.10.19")
    testCompile("org.powermock", "powermock-mockito-release-full", "1.6.4")
}

(tasks["test"] as Test).apply {
    systemProperty("clickhouseUrl", System.getenv("clickhouseUrl"))
    systemProperty("clickhouseUser", System.getenv("clickhouseUser"))
    systemProperty("clickhousePassword", System.getenv("clickhousePassword"))

    useTestNG()
}


val sourceJar = task<Jar>("sourceJar") {
    classifier = "sources"
    from(kotlin.sourceSets["main"]!!.kotlin.sourceDirectories)
}

publishing {
    publications.invoke {
        "MavenJava"(MavenPublication::class) {
            artifactId = rootProject.name

            from(components.getByName("java"))
            artifact(sourceJar)
        }
    }
}

artifactory {
    setContextUrl("https://oss.jfrog.org/artifactory")

    publish(delegateClosureOf<PublisherConfig> {
        repository(delegateClosureOf<GroovyObject> {
            setProperty("repoKey", "oss-snapshot-local")
            setProperty("username", "tanvd")
            setProperty("password", System.getenv("artifactory_api_key"))
            setProperty("maven", true)
        })

        defaults(delegateClosureOf<GroovyObject> {
            setProperty("publishArtifacts", true)
            setProperty("publishPom", true)
            invokeMethod("publications", "MavenJava")
        })
    })
}


bintray {
    user = "tanvd"
    key = System.getenv("bintray_api_key")
    publish = true
    setPublications("MavenJava")
    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "jetaudit"
        name = rootProject.name
        githubRepo = "tanvd/jetaudit"
        vcsUrl = "https://github.com/tanvd/jetaudit"
        setLabels("kotlin", "clickhouse", "audit")
        setLicenses("MIT")
        desc = "Kotlin library for business audit upon Clickhouse"
    })
}

task<Wrapper>("wrapper") {
    gradleVersion = "4.9"
}

