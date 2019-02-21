import tanvd.kosogor.proxy.publishJar

group = "tanvd.jetaudit"
version = "1.1.4-SNAPSHOT"

plugins {
    kotlin("jvm") version "1.3.21" apply true
    id("tanvd.kosogor") version "1.0.0" apply true
}

repositories {
    jcenter()
    maven("https://dl.bintray.com/jfrog/jfrog-jars")
}

dependencies {
    compile("org.slf4j", "slf4j-api", "1.7.25")

    compile(kotlin("stdlib"))
    compile(kotlin("reflect"))

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

publishJar {
    publication {
        artifactId = "jetaudit"
    }

    artifactory {
        serverUrl = "https://oss.jfrog.org/artifactory"
        repository = "oss-snapshot-local"
        username = "tanvd"
        secretKey = System.getenv("artifactory_api_key")
    }

    bintray {
        username = "tanvd"
        secretKey = System.getenv("bintray_api_key")
        repository = "jetaudit"
        info {
            githubRepo = "tanvd/jetaudit"
            vcsUrl = "https://github.com/tanvd/jetaudit"
            labels.addAll(listOf("kotlin", "clickhouse", "audit"))
            license = "MIT"
            description = "Kotlin library for business audit upon Clickhouse"
        }
    }
}
