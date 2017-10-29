package tanvd.audit.implementation.dao

import tanvd.aorm.query.LimitExpression
import tanvd.aorm.query.OrderByExpression
import tanvd.aorm.query.QueryExpression
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.implementation.exceptions.BasicDbException
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
    fun <T : Any> addInformationInDbModel(information: InformationType<T>)

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
    fun loadRecords(expression: QueryExpression, limitExpression: LimitExpression? = null,
                    orderByExpression: OrderByExpression? = null): List<AuditRecordInternal>

    /**
     * Return number of records satisfying expression.
     *
     * Implementations must implement it with built in Db functions. (it means, that implementations must not count
     * number of records in result set of query returned by Db)
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun countRecords(expression: QueryExpression): Long

    /**
     * Closes all connections and prepares for shutdown.
     */
//    fun finalize()

    /**
     * Resets audit table
     */
    @Throws(BasicDbException::class)
    fun resetTable()
}