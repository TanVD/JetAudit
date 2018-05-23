package utils

import ru.yandex.clickhouse.ClickHouseDataSource
import java.util.*
import javax.sql.DataSource

object DbUtils {
    fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty("user", "default")
        properties.setProperty("password", System.getProperty("clickhousePassword")!!.trim('"')) // replace it with real value for local executions
        properties.setProperty("Url",/* System.getProperty("ClickhouseUrl")?.trim('"') ?: */"jdbc:clickhouse://intdevsrv3.labs.intellij.net:8123")
        return properties
    }

    fun getDataSource(): DataSource {
        val url = getProperties()["Url"].toString()
        return ClickHouseDataSource(url, getProperties())
    }
}