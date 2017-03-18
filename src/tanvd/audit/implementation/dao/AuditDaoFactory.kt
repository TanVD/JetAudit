package tanvd.audit.implementation.dao

import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import java.sql.DriverManager

object AuditDaoFactory {
    var connectionUrl : String? = null

    var user : String? = null

    var password : String? = null

    var dbName : DbName = DbName.Clickhouse

    fun getDao() : AuditDao {
        val rawConnection = DriverManager.getConnection(connectionUrl, user, password)
        val auditDao : AuditDao
        when (dbName) {
            DbName.Clickhouse -> {
                auditDao = AuditDaoClickhouseImpl(rawConnection)
            }
            DbName.MySQL -> {
                auditDao = AuditDaoMysqlImpl(rawConnection)
            }
        }
        return auditDao
    }

}