package utils

import tanvd.audit.model.internal.db.DbCredentials
import java.util.*

object DbUtils {
    fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty("Username", "default")
        properties.setProperty("Password", "")
        properties.setProperty("Url", "jdbc:clickhouse://localhost:8123/example")
        return properties
    }

    fun getCredentials(): DbCredentials {
        return DbCredentials("default", "", "jdbc:clickhouse://localhost:8123/example")
    }
}