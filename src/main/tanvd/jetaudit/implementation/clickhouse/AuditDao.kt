package tanvd.jetaudit.implementation.clickhouse

import tanvd.aorm.exceptions.BasicDbException
import tanvd.aorm.query.LimitExpression
import tanvd.aorm.query.OrderByExpression
import tanvd.aorm.query.QueryExpression
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.internal.AuditRecordInternal

internal interface AuditDao {

    /**
     * Creates AuditTable, if useDDL = true
     */
    fun initTable()

    /**
     * Saves audit record and all its objects
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun saveRecord(auditRecordInternal: AuditRecordInternal)

    /**
     * Saves audit records. Makes it faster, than FOR loop with saveRecord
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun saveRecords(auditRecordInternals: List<AuditRecordInternal>)

    /**
     * Adds new object type and creates columns for it
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun <T : Any> addTypeInDbModel(type: ObjectType<T>)

    /**
     * Adds new information type and creates column for it
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun <T : Any> addInformationInDbModel(information: InformationType<T>)

    /**
     * Loads all auditRecords with specified object except Deleted
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun loadRecords(expression: QueryExpression, orderByExpression: OrderByExpression? = null,
                    limitExpression: LimitExpression? = null): List<AuditRecordInternal>

    /**
     * Return total count of records satisfying condition except Deleted
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun countRecords(expression: QueryExpression): Long
}