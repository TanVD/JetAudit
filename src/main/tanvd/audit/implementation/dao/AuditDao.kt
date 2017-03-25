package tanvd.audit.implementation.dao

import tanvd.audit.exceptions.UninitializedException
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import javax.sql.DataSource

interface AuditDao {
    fun saveRecord(auditRecord: AuditRecord)

    fun saveRecords(auditRecords: List<AuditRecord>)

    fun <T> addTypeInDbModel(type: AuditType<T>)

    fun <T> loadRecords(type: AuditType<T>, id: String): List<AuditRecord>

    companion object AuditDaoFactory {
        private var connectionUrl: String? = null

        private var user: String? = null

        private var password: String? = null

        private var dbType: DbType = DbType.Clickhouse

        private var dataSource: DataSource? = null

        fun setConfig(dbType: DbType, connectionUrl: String, user: String, password: String) {
            this.dbType = dbType
            this.connectionUrl = connectionUrl
            this.user = user
            this.password = password
        }

        fun setConfig(dbType: DbType, dataSource: DataSource) {
            this.dbType = dbType
            this.dataSource = dataSource
        }

        @Throws(UninitializedException::class)
        fun getDao(): AuditDao {
            if (dataSource != null) {
                return dbType.getDao(dataSource!!)
            } else if (connectionUrl != null && user != null && password != null) {
                return dbType.getDao(connectionUrl!!, user!!, password!!)
            } else {
                throw UninitializedException("AuditDaoFactory not initialized, but getDao called")
            }
        }

    }

}