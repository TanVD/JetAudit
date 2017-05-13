package tanvd.audit.implementation.dao

import tanvd.audit.exceptions.UninitializedException
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.model.external.queries.QueryExpression
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal
import javax.sql.DataSource


internal interface AuditDao {

    /**
     * Saves specified record in Db or throws exception.
     *
     * @throws BasicDbException
     */
    fun saveRecord(auditRecordInternal: AuditRecordInternal)

    /**
     * Saves specified records in Db or throws exception.
     *
     * In most cases bath saving works faster than for loop.
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun saveRecords(auditRecordInternals: List<AuditRecordInternal>)

    /**
     * Performs operations on Db scheme needed to support new audit type.
     *
     * @throws BasicDbException
     */
    fun <T : Any> addTypeInDbModel(type: ObjectType<T>)

    /**
     * Performs operations on Db scheme needed to support new information type.
     *
     * @throws BasicDbException
     */
    fun <T> addInformationInDbModel(information: InformationType<T>)

    /**
     * Load records satisfying expression limited by specified parameters.
     *
     * Implementations must pass parameters of query to Db (it means, that implementations must not apply parameters
     * to result set of query returned by Db)
     *
     * @throws UnknownObjectTypeException
     * @throws BasicDbException
     */
    fun loadRecords(expression: QueryExpression, parameters: QueryParameters): List<AuditRecordInternal>

    /**
     * Return number of records satisfying expression.
     *
     * Implementations must implement it with built in Db functions. (it means, that implementations must not count
     * number of records in result set of query returned by Db)
     *
     * @throws BasicDbException
     */
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