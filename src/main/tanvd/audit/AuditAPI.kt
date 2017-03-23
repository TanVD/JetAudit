package tanvd.audit

import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.dao.AuditDaoFactory
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.LongSerializer
import tanvd.audit.serializers.StringSerializer
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import javax.sql.DataSource
import kotlin.reflect.KClass
import tanvd.audit.exceptions.UnknownAuditTypeException

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
    //Factory for auditDao
    val auditDao : AuditDao

    val executor: AuditExecutor

    val auditQueue: LinkedBlockingQueue<AuditRecord> = LinkedBlockingQueue()

    /**
     * Create AuditApi with default dataSource
     */
    constructor(dbType: DbType, connectionUrl : String, user : String, password : String) {
        AuditDaoFactory.setConfig(dbType, connectionUrl, user, password)
        auditDao = AuditDaoFactory.getDao()

        executor = AuditExecutor(auditQueue)

        addTypeForAudit(AuditType(String::class, "Type_String", StringSerializer))
        addTypeForAudit(AuditType(Int::class, "Type_Int", IntSerializer))
        addTypeForAudit(AuditType(Long::class, "Type_Long", LongSerializer))
    }

    /**
     * Create AuditApi with your dataSource
     */
    constructor(dbType : DbType, dataSource : DataSource) {
        AuditDaoFactory.setConfig(dbType, dataSource)
        auditDao = AuditDaoFactory.getDao()

        executor = AuditExecutor(auditQueue)

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
    fun saveAudit(vararg objects: Any) {
        val recordObjects = ArrayList<Pair<AuditType<Any>, String>>()
        for (o in objects) {
            val type = AuditType.resolveType(o.javaClass.kotlin)
            val objectId = type.serialize(o)
            recordObjects.add(Pair(type, objectId))
        }

        auditQueue.put(AuditRecord(recordObjects))
    }

    /**
     * Load audits containing specified object
     *
     * @throws UnknownAuditTypeException
     */
    fun loadAudit(klass: KClass<*>, idToLoad: String): MutableList<MutableList<Any>> {
        val auditRecords = auditDao.loadRecords(AuditType.resolveType(klass), idToLoad)
        val resultList = ArrayList<MutableList<Any>>()

        for (auditRecord in auditRecords) {
            val currentRec = ArrayList<Any>()
            for (o in auditRecord.objects) {
                val (type, id) = o
                val objectRes = type.deserialize(id)
                currentRec.add(objectRes)
            }
            resultList.add(currentRec)
        }
        return resultList
    }

}