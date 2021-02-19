package tanvd.jetaudit.utils

import ru.yandex.clickhouse.ClickHouseDataSource
import java.util.*
import javax.sql.DataSource

object DbUtils {
    const val testInsertWorkerDelayMs = 2000L
    private const val containerPort = 8123

    private val localstack = KGenericContainer("yandex/clickhouse-server:21.1")
            .withExposedPorts(containerPort)
            .apply { start() }

    fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty("user", "default")
        properties.setProperty("password", "")
        properties.setProperty("Url", "jdbc:clickhouse://localhost:${localstack.getMappedPort(containerPort)}")
        return properties
    }

    fun getDataSource(): DataSource {
        val url = getProperties()["Url"].toString()
        return ClickHouseDataSource(url, getProperties())
    }
}
