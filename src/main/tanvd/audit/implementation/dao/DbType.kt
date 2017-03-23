package tanvd.audit.implementation.dao

import com.mysql.cj.jdbc.MysqlDataSource
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import javax.sql.DataSource

enum class DbType {
    /**
     * Default Dao for Clickhouse
     */
    Clickhouse {
        override fun getDao(dataSource: DataSource) : AuditDao {
            return AuditDaoClickhouseImpl(dataSource)
        }

        override fun getDao(connectionUrl : String, username : String, password : String) : AuditDao {
            val properties = ClickHouseProperties()
            properties.user = username
            properties.password = password
            val dataSource = ClickHouseDataSource(connectionUrl, properties)
            return AuditDaoClickhouseImpl(dataSource)
        }
    },
    /**
     * Default Dao for MySQL
     */
    MySQL {

        override fun getDao(dataSource: DataSource) : AuditDao {
            return AuditDaoMysqlImpl(dataSource)
        }

        override fun getDao(connectionUrl : String, username : String, password : String) : AuditDao {
            val dataSource = MysqlDataSource()
            dataSource.setURL(connectionUrl)
            dataSource.user = username
            dataSource.setPassword(password)
            return AuditDaoMysqlImpl(dataSource)
        }
    };

    abstract fun getDao(dataSource: DataSource) : AuditDao

    abstract fun getDao(connectionUrl : String, username : String, password : String) : AuditDao

}