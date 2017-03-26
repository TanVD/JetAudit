package tanvd.audit

import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.external.QueryExpression
import tanvd.audit.model.external.QueryParameters
import tanvd.audit.model.internal.AuditRecord
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.LongSerializer
import tanvd.audit.serializers.StringSerializer
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import javax.sql.DataSource

/**
 * Asynchronous saving of connected entities.
 *
 * Saves ID's of entities in arrays, it's order and also directly saves all strings in array.
 *
 * So, let's imagine you got entities WOLF (id = Jack), FOX (id = Lisa), GIRL (id = RedHat) and
 * you got saving (returns ID) and loading (parameter -- ID, returns entity) functions working
 * in some other persistent context. DbArrayString [GIRL, "robbed by", FOX, "and", WOLF] will be saved as
 * "RedHat robbed by Lisa and Jack" (with some simplification) where ID's are retrieved with appropriate
 * saving function. After it you can load original array, all ID's will be resolved to objects with
 * appropriate loading function.
 *
 * Pay attention, that normal work of AuditAPI depends on external persistent context.
 */
class AuditAPI {
    internal val auditDao: AuditDao

    internal val executor: AuditExecutor

    internal val auditQueue: BlockingQueue<AuditRecord>

    /**
     * Create AuditApi with default dataSource
     *
     * If queue will be overfilled all new audit  records will be lost
     */
    constructor(queueCapacity: Int, dbType: DbType, connectionUrl: String, user: String, password: String) {
        auditQueue = ArrayBlockingQueue(queueCapacity)

        AuditDao.setConfig(dbType, connectionUrl, user, password)
        auditDao = AuditDao.getDao()

        executor = AuditExecutor(auditQueue)

        initTypes()
    }

    /**
     * Create AuditApi with your dataSource
     *
     * If queue will be overfilled all new audit  records will be lost
     */
    constructor(queueCapacity: Int, dbType: DbType, dataSource: DataSource) {
        auditQueue = ArrayBlockingQueue(queueCapacity)

        AuditDao.setConfig(dbType, dataSource)
        auditDao = AuditDao.getDao()

        executor = AuditExecutor(auditQueue)

        initTypes()
    }

    private fun initTypes() {
        addTypeForAudit(AuditType(String::class, "Type_String", StringSerializer))
        addTypeForAudit(AuditType(Int::class, "Type_Int", IntSerializer))
        addTypeForAudit(AuditType(Long::class, "Type_Long", LongSerializer))
    }

    /**
     * Add types for audit saving.
     */
    fun <T> addTypeForAudit(type: AuditType<T>) {
        auditDao.addTypeInDbModel(type)
        synchronized(AuditType) {
            @Suppress("UNCHECKED_CAST")
            AuditType.addType(type as AuditType<Any>)
        }
    }

    /**
     * Save audit entry resolving dependencies.
     * Unknown types will reported with exception
     * Audit will be saved after buffer of worker will be filled
     *
     * @throws UnknownAuditTypeException
     */
    @Throws(UnknownAuditTypeException::class)
    fun saveAudit(vararg objects: Any, unixTimeStamp: Int) {
        val recordObjects = ArrayList<Pair<AuditType<Any>, String>>()
        for (o in objects) {
            val type = AuditType.resolveType(o.javaClass.kotlin)
            val objectId = type.serialize(o)
            recordObjects.add(Pair(type, objectId))
        }

        auditQueue.offer(AuditRecord(recordObjects, unixTimeStamp))
    }

    /**
     * Load audits containing specified object. Supports paging and ordering
     *
     * @throws UnknownAuditTypeException
     */
    @Throws(UnknownAuditTypeException::class)
    fun loadAudit(expression: QueryExpression, parameters: QueryParameters): MutableList<MutableList<Any>> {

        val auditRecords = auditDao.loadRecords(expression, parameters)
        val resultList = ArrayList<MutableList<Any>>()

        for ((objects) in auditRecords) {
            val currentRec = ArrayList<Any>()
            for (o in objects) {
                val (type, id) = o
                val objectRes = type.deserialize(id)
                currentRec.add(objectRes)
            }
            resultList.add(currentRec)
        }
        return resultList
    }

    /**
     * Stop audit saving
     */
    fun stopAudit(timeToWait: Long): Boolean {
        return executor.stopWorkers(timeToWait)
    }

}