package tanvd.jetaudit.utils

import org.testcontainers.containers.ClickHouseContainer
import ru.yandex.clickhouse.ClickHouseDataSource
import java.util.*
import javax.sql.DataSource

object DbUtils {
    private val localstack = ClickHouseContainer("yandex/clickhouse-server:21.1").apply { start() }

    fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty("user", localstack.username)
        properties.setProperty("password", localstack.password)
        properties.setProperty("Url", localstack.jdbcUrl)
        return properties
    }

    fun getDataSource(): DataSource {
        val url = getProperties()["Url"].toString()
        return ClickHouseDataSource(url, getProperties())
    }
}
