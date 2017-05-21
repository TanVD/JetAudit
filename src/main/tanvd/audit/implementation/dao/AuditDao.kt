package tanvd.audit.implementation.dao

import tanvd.audit.exceptions.UninitializedException
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.model.external.db.DbProperties
import tanvd.audit.model.external.db.DbType
import tanvd.audit.model.external.queries.QueryExpression
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal


internal interface AuditDao {

    /**
     * Saves specified record in Db or throws exception.
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
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
    @Throws(BasicDbException::class)
    fun <T : Any> addTypeInDbModel(type: ObjectType<T>)

    /**
     * Performs operations on Db scheme needed to support new information type.
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
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
    @Throws(BasicDbException::class)
    fun loadRecords(expression: QueryExpression, parameters: QueryParameters): List<AuditRecordInternal>

    /**
     * Return number of records satisfying expression.
     *
     * Implementations must implement it with built in Db functions. (it means, that implementations must not count
     * number of records in result set of query returned by Db)
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun countRecords(expression: QueryExpression): Int

    companion object AuditDaoFactory {

        private var dbProperties: DbProperties? = null

        private var dbType: DbType = DbType.Clickhouse

        fun setConfig(dbType: DbType, dbProperties: DbProperties) {
            this.dbType = dbType
            this.dbProperties = dbProperties
        }

        @Throws(UninitializedException::class)
        fun getDao(): AuditDao {
            if (dbProperties != null) {
                return dbType.getDao(dbProperties!!)
            }
            throw UninitializedException("DbProperties not initialized")
        }

    }

}