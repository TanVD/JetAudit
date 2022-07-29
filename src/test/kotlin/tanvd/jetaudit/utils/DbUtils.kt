package tanvd.jetaudit.utils

import com.clickhouse.client.config.ClickHouseDefaults
import com.clickhouse.jdbc.ClickHouseDataSource
import org.testcontainers.containers.ClickHouseContainer
import java.util.*
import javax.sql.DataSource

object DbUtils {
    private val localstack by lazy {
        ClickHouseContainer("clickhouse/clickhouse-server:22.3.8.39-alpine").apply {
            start()
        }
    }

    fun getProperties(): Properties {
        val properties = Properties()
        properties[ClickHouseDefaults.USER] = localstack.username
        properties[ClickHouseDefaults.PASSWORD] =  localstack.password
        return properties
    }

    fun getDataSource(): DataSource {
        return ClickHouseDataSource(localstack.jdbcUrl, getProperties())
    }
}
