package tanvd.audit.implementation.dao

import javax.sql.DataSource

object AuditDaoFactory {
    var connectionUrl : String? = null

    var user : String? = null

    var password : String? = null

    var dbType : DbType = DbType.Clickhouse

    var dataSource : DataSource? = null

    fun setConfig(dbType: DbType, connectionUrl : String, user : String, password : String) {
        this.dbType = dbType
        this.connectionUrl = connectionUrl
        this.user = user
        this.password = password
    }

    fun setConfig(dbType: DbType, dataSource: DataSource) {
        this.dbType = dbType
        this.dataSource = dataSource
    }

    fun getDao() : AuditDao {
        if (dataSource != null) {
            return dbType.getDao(dataSource!!)
        } else {
            return dbType.getDao(connectionUrl!!, user!!, password!!)
        }
    }

}