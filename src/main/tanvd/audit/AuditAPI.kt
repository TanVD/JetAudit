package tanvd.audit

import org.slf4j.LoggerFactory
import tanvd.audit.exceptions.AddExistingAuditTypeException
import tanvd.audit.exceptions.AuditQueueFullException
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
 * Asynchronous saving of entities.
 *
 *
 * Saves ID's of entities in arrays, it's order and also directly saves all primitive types in array. Also saves
 * unixtimestamp. If timestamp not set AuditApi will generate it using System.currentTimeMillis()
 *
 * Primitive types: String, Long, Int
 *
 * You can configure AuditApi and Clickhouse scheme using properties file.
 *
 * Use audit.config system property to specify path to file (relative or absolute)
 *
 * Configuration may include:
 *      #AuditApi config
 *      CapacityOfQueue        (default 20000),
 *      NumberOfWorkers        (default 5),
 *      CapacityOfWorkerBuffer (default 5000),
 *      CapacityOfQueue        (default 20000),
 *      WaitingQueueTime       (default 10)
 *
 *      #Reserving config
 *      MaxGeneration          (default 15)
 *      ReserveFilePath        (default reserve.txt)
 *
 *      #Clickouse scheme config
 *      AuditTable             (default Audit),
 *      DescriptionColumn      (default Description),
 *      DateColumn             (default Audit_Date),
 *      UnixTimeStampColumn    (default Unix_TimeStamp)
 *
 * If properties file or some properties not found default values will be used.
 *
 * Pay attention, that normal work of AuditAPI depends on external persistent context.
 *
 *
 * You can either use methods which throw exception (to be confident that audit records was saved),
 * or use methods logging exceptions (to be confident that exception will not crash calling thread).
 *
 * In case of method with exception JetAudit guarantee that record will be saved or exception will be thrown.
 * In other case JetAudit tries to save record, but not guarantee that it will be saved in exceptional situations.
 */
class AuditAPI {

    private val logger = LoggerFactory.getLogger(AuditAPI::class.java)

    internal val auditDao: AuditDao

    internal val executor: AuditExecutor

    /**
     * If queue will be overfilled all new audit records will be lost
     */
    internal val auditQueueInternal: BlockingQueue<AuditRecordInternal>

    internal companion object Config {
        val capacityOfQueue = PropertyLoader.loadProperty("CapacityOfQueue")?.toInt() ?: 20000
    }

    /**
     * Create AuditApi with default dataSource
     */
    constructor(dbType: DbType, connectionUrl: String, user: String, password: String) {
        auditQueueInternal = ArrayBlockingQueue(capacityOfQueue)

        AuditDao.setConfig(dbType, connectionUrl, user, password)
        auditDao = AuditDao.getDao()

        executor = AuditExecutor(auditQueueInternal)

        addPrimitiveTypes()
    }

    /**
     * Create AuditApi with your dataSource
     */
    constructor(dbType: DbType, dataSource: DataSource) {
        auditQueueInternal = ArrayBlockingQueue(capacityOfQueue)

        AuditDao.setConfig(dbType, dataSource)
        auditDao = AuditDao.getDao()

        executor = AuditExecutor(auditQueueInternal)

        addPrimitiveTypes()
    }


    /**
     * Constructor for test needs
     */
    internal constructor(auditDao: AuditDao, executor: AuditExecutor, auditQueueInternal: BlockingQueue<AuditRecordInternal>) {
        this.auditQueueInternal = auditQueueInternal;
        this.auditDao = auditDao
        this.executor = executor
    }

    /**
     * Initializing type system with primitive types
     */
    internal fun addPrimitiveTypes() {
        addTypeForAudit(AuditType(String::class, "Type_String", StringSerializer))
        addTypeForAudit(AuditType(Int::class, "Type_Int", IntSerializer))
        addTypeForAudit(AuditType(Long::class, "Type_Long", LongSerializer))
    }

    /**
     * Add types of audit entities to JetAudit type system
     *
     * In case of AuditType duplicate
     * @throws AddExistingAuditTypeException
     */
    fun <T> addTypeForAudit(type: AuditType<T>) {
        if (AuditType.getTypes().contains(type as AuditType<*>)) {
            throw AddExistingAuditTypeException("Already existing type requested to add -- ${type.code}")
        }
        auditDao.addTypeInDbModel(type)
        synchronized(AuditType) {
            @Suppress("UNCHECKED_CAST")
            AuditType.addType(type as AuditType<Any>)
        }
    }

    /**
     * Save audit entry resolving dependencies.
     *
     * This method not throwing any exceptions.
     * Unknown types will be ignored and reported with log error.
     */
    fun saveAudit(vararg objects: Any, unixTimeStamp: Long = System.currentTimeMillis()) {
        val recordObjects = ArrayList<Pair<AuditType<Any>, String>>()
        for (o in objects) {
            try {
                val type = AuditType.resolveType(o::class)
                recordObjects.add(Pair(type, type.serialize(o)))
            } catch (e: UnknownAuditTypeException) {
                logger.error("AuditAPI met unknown AuditType. Object will be ignored")
            }
        }

        val isFull = !auditQueueInternal.offer(AuditRecordInternal(recordObjects, unixTimeStamp))
        if (isFull) {
            logger.error("AuditQueue is full in AuditAPI. Some audit records may be lost.")
        }
    }

    /**
     * Save audit entry resolving dependencies.
     *
     * This method throws exceptions related to AuditApi. Exceptions of Db are ignored anyway.
     *
     * @throws UnknownAuditTypeException
     * @throws AuditQueueFullException
     */
    fun saveAuditWithExceptions(vararg objects: Any, unixTimeStamp: Long = System.currentTimeMillis()) {
        val recordObjects = objects.map { o -> AuditType.resolveType(o::class).let { it to it.serialize(o) } }

        val isFull = !auditQueueInternal.offer(AuditRecordInternal(recordObjects, unixTimeStamp))
        if (isFull) {
            throw AuditQueueFullException("Audit queue full. Audit record was ignored.")
        }
    }

    /**
     * Load audits containing specified object. Supports paging and ordering.
     *
     * This method not throwing any exceptions.
     */
    fun loadAudit(expression: QueryExpression, parameters: QueryParameters): List<AuditRecord> {
        val auditRecords: List<AuditRecordInternal>
        try {
            auditRecords = auditDao.loadRecords(expression, parameters)
        } catch (e: UnknownAuditTypeException) {
            logger.error("AuditAPI met unknown AuditType. Empty list will be returned.")
            return emptyList()
        }

        val resultList = deserializeAuditRecords(auditRecords)

        return resultList
    }

    /**
     * Load audits containing specified object. Supports paging and ordering.
     *
     * This method throws exceptions related to AuditApi. Exceptions of Db are ignored anyway.
     *
     * @throws UnknownAuditTypeException
     */
    fun loadAuditWithExceptions(expression: QueryExpression, parameters: QueryParameters): List<AuditRecord> {
        val auditRecords: List<AuditRecordInternal> = auditDao.loadRecords(expression, parameters)

        val resultList = deserializeAuditRecords(auditRecords)

        return resultList
    }

    /**
     * Deserialize AuditRecordsInternal to AuditRecords using batching deserialization.
     */
    private fun deserializeAuditRecords(auditRecords: List<AuditRecordInternal>): List<AuditRecord> {
        val preparedForBatchDeserialization = auditRecords.flatMap { it.objects }.groupBy { it.first }
                .mapValues { it.value.map { it.second }.distinct() }

        val deserializedMaps = preparedForBatchDeserialization.mapValues { it.key.serializer.deserializeBatch(it.value) }

        val resultList = auditRecords.map { (objects, timeStamp) ->
            AuditRecord(objects.map {
                (type, id) ->
                deserializedMaps[type]!![id]!!.let { AuditObject(type, type.display(it), it) }
            }, timeStamp)
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