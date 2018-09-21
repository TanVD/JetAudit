import com.jfrog.bintray.gradle.BintrayExtension

group = "tanvd.jetaudit"
version = "1.1.0"

val kotlinVersion = "1.2.70"

plugins {
    kotlin("jvm") version "1.2.70" apply true
    `maven-publish` apply true
    id("com.jfrog.bintray") version "1.8.4" apply true
}

repositories {
    mavenCentral()
    maven { setUrl("https://dl.bintray.com/jfrog/jfrog-jars") }
    maven { setUrl("https://dl.bintray.com/tanvd/aorm") }
    jcenter()

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

    compile("tanvd.aorm", "aorm", "1.1.0")
    compile("com.amazonaws", "aws-java-sdk-s3", "1.11.160")

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

