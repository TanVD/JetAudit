package tanvd.aorm.implementation

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import java.sql.Connection
import javax.sql.DataSource

object ConnectionClikhouse {

    val dataSource: DataSource

    init {
        val properties = ClickHouseProperties()
        properties.user = "default"
        properties.password = ""
        dataSource = ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties)

    }

    fun <T>withConnection(body: Connection.() -> T) : T {
        return dataSource.connection.use {
            it.body()
        }
    }

    fun execute(sql: String) {
        withConnection {
            prepareStatement(sql).use {
                it.execute()
            }
        }
    }
}
