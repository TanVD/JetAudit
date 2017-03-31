package tanvd.audit

import org.slf4j.LoggerFactory
import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.*
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.LongSerializer
import tanvd.audit.serializers.StringSerializer
import tanvd.audit.utils.PropertyLoader
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

    private val logger = LoggerFactory.getLogger(AuditAPI::class.java)

    internal val auditDao: AuditDao

    internal val executor: AuditExecutor

    internal val auditQueueInternal: BlockingQueue<AuditRecordInternal>

    companion object Config {
        val capacityOfQueue = PropertyLoader.load("auditApiConfig.properties", "CapacityOfQueue").toInt()
    }


    /**
     * Create AuditApi with default dataSource
     *
     * If queue will be overfilled all new audit  records will be lost
     */
    constructor(dbType: DbType, connectionUrl: String, user: String, password: String) {
        auditQueueInternal = ArrayBlockingQueue(capacityOfQueue)

        AuditDao.setConfig(dbType, connectionUrl, user, password)
        auditDao = AuditDao.getDao()

        executor = AuditExecutor(auditQueueInternal)

        initTypes()
    }

    /**
     * Create AuditApi with your dataSource
     *
     * If queue will be overfilled all new audit  records will be lost
     */
    constructor(dbType: DbType, dataSource: DataSource) {
        auditQueueInternal = ArrayBlockingQueue(capacityOfQueue)

        AuditDao.setConfig(dbType, dataSource)
        auditDao = AuditDao.getDao()

        executor = AuditExecutor(auditQueueInternal)

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
    fun saveAudit(vararg objects: Any, unixTimeStamp: Long = System.currentTimeMillis()) {
        val recordObjects = objects.map { o -> AuditType.resolveType(o::class).let { it to it.serialize(o) } }

        val isFull = !auditQueueInternal.offer(AuditRecordInternal(recordObjects, unixTimeStamp))
        if (isFull) {
            logger.error("AuditQueue is full in AuditAPI. Some audit records may be lost.")
        }
    }

    /**
     * Load audits containing specified object. Supports paging and ordering
     *
     * @throws UnknownAuditTypeException
     */
    @Throws(UnknownAuditTypeException::class)
    fun loadAudit(expression: QueryExpression, parameters: QueryParameters): List<AuditRecord> {

        val auditRecords = auditDao.loadRecords(expression, parameters)

        val preparedForBatchDeserialization = auditRecords.flatMap { it.objects }.groupBy { it.first }
                .mapValues { it.value.map { it.second }.distinct() }

        val deserializedMaps = preparedForBatchDeserialization.mapValues { it.key.serializer.deserializeBatch(it.value) }

        val resultList = auditRecords.map { (objects) ->
            AuditRecord(objects.map {
                (type, id) ->
                deserializedMaps[type]!![id]!!.let { AuditObject(type, type.display(it), it) }
            })
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