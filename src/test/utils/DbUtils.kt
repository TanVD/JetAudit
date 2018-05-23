package utils

import ru.yandex.clickhouse.ClickHouseDataSource
import java.util.*
import javax.sql.DataSource

object DbUtils {
    fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty("user", System.getProperty("clickhouseUser")?.takeIf(String::isNotBlank)?: "default")
        properties.setProperty("password", System.getProperty("clickhousePassword")?.takeIf(String::isNotBlank)) // replace it with real value for local executions or path as system param
        properties.setProperty("Url", System.getProperty("clickhouseUrl")?.takeIf(String::isNotBlank)?: "jdbc:clickhouse://intdevsrv3.labs.intellij.net:8123")
        return properties
    }

    fun getDataSource(): DataSource {
        val url = getProperties()["Url"].toString()
        return ClickHouseDataSource(url, getProperties())
    }
}