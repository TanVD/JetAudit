package tanvd.audit.implementation.dao

import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import java.sql.DriverManager

object AuditDaoFactory {
    var connectionUrl : String? = null

    var user : String? = null

    var password : String? = null

    fun getDao() : AuditDao {
        val rawConnection = DriverManager.getConnection(connectionUrl, user, password)
        val auditDao = AuditDaoMysqlImpl(rawConnection)
        return auditDao
    }

}