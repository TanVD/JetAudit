package tanvd.audit.implementation.dao

import tanvd.audit.exceptions.UninitializedException
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.external.QueryExpression
import tanvd.audit.model.external.QueryParameters
import tanvd.audit.model.internal.AuditRecord
import javax.sql.DataSource

internal interface AuditDao {
    fun saveRecord(auditRecord: AuditRecord)

    fun saveRecords(auditRecords: List<AuditRecord>)

    fun <T> addTypeInDbModel(type: AuditType<T>)

    fun loadRecords(expression: QueryExpression, parameters: QueryParameters): List<AuditRecord>

    fun countRecords(expression: QueryExpression): Int

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