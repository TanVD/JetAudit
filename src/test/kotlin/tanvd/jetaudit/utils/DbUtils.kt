package tanvd.jetaudit.utils

import com.clickhouse.client.api.ClientConfigProperties
import com.clickhouse.jdbc.DataSourceImpl
import org.testcontainers.clickhouse.ClickHouseContainer
import java.util.*
import javax.sql.DataSource

object DbUtils {
    private val localstack by lazy {
        ClickHouseContainer("clickhouse/clickhouse-server:22.3.8.39").withTmpFs(mapOf("/var/lib/clickhouse" to "rw")).apply {
            start()
        }
    }

    fun getProperties(): Properties {
        val properties = Properties()
        properties[ClientConfigProperties.USER.key] = localstack.username
        properties[ClientConfigProperties.PASSWORD.key] =  localstack.password
        return properties
    }

    fun getDataSource(): DataSource {
        return DataSourceImpl(localstack.jdbcUrl, getProperties())
    }
}
