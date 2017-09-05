package utils

import tanvd.audit.model.internal.db.DbCredentials
import java.util.*

object DbUtils {
    fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty("Username", "default")
        properties.setProperty("Password", "")
        properties.setProperty("Url", System.getProperty("ClickhouseUrl")?.trim('"') ?: "jdbc:clickhouse://intdevsrv3.labs.intellij.net:8123")
        return properties
    }

    fun getCredentials(): DbCredentials {
        return DbCredentials("default", "",  System.getProperty("ClickhouseUrl")?.trim('"') ?: "jdbc:clickhouse://intdevsrv3.labs.intellij.net:8123")
    }
}