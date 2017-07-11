package tanvd.audit.model.external.db

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.audit.exceptions.UninitializedException
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao

enum class DbType {
    /**
     * Default Dao for Clickhouse
     *
     * @throws UninitializedException
     */
    Clickhouse {
        override fun getDao(dbProperties: DbProperties): AuditDao {
            if (dbProperties.isDataSourceSet()) {
                return AuditDaoClickhouseImpl(dbProperties.dataSource!!, dbProperties)
            } else if (dbProperties.isCredentialsSet()) {
                val properties = ClickHouseProperties()
                properties.user = dbProperties.user
                properties.password = dbProperties.password
                return AuditDaoClickhouseImpl(ClickHouseDataSource(dbProperties.connectionUrl, properties), dbProperties)
            }
            throw UninitializedException("Nor credentials nor datasource set in DbProperties")
        }
    };

    /**
     * Get DAO by dbProperties
     */
    internal abstract fun getDao(dbProperties: DbProperties): AuditDao
}