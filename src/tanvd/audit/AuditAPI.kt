package tanvd.audit

import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.dao.AuditDaoFactory
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.LongSerializer
import tanvd.audit.serializers.StringSerializer
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import kotlin.reflect.KClass

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
class AuditAPI(connectionUrl : String, user : String, password : String) {
    //Factory for auditDao
    val auditDao : AuditDao

    val executor: AuditExecutor

    val auditQueue: LinkedBlockingQueue<AuditRecord> = LinkedBlockingQueue()

    init {
        AuditDaoFactory.connectionUrl = connectionUrl
        AuditDaoFactory.user = user
        AuditDaoFactory.password = password

        auditDao = AuditDaoFactory.getDao()

        executor = AuditExecutor(auditQueue)

        addTypeForAudit(AuditType(String::class, "Type_String", StringSerializer))
        addTypeForAudit(AuditType(Int::class, "Type_Int", IntSerializer))
        addTypeForAudit(AuditType(Long::class, "Type_Long", LongSerializer))
    }

    /**
     * Add types for audit saving.
     * You need to specify code for audit name
     */
    fun <T> addTypeForAudit(type: AuditType<T>) {
        @Suppress("UNCHECKED_CAST")
        AuditType.addType(type as AuditType<Any>)
        auditDao.addType(type)
    }

    /**
     * Save audit entry resolving dependencies.
     * Unknown types will be ignored (for now).
     * Audit will be saved after buffer of worker will be filled
     */
    fun saveAudit(vararg objects: Any) {
        val audit = AuditRecord()
        for (o in objects) {
            //TODO -- add exceptions
            val type = AuditType.resolveType(o.javaClass.kotlin)
            val objectId = type.serializer.serialize(o)
            audit.objects.add(Pair(type, objectId))
        }

        auditQueue.put(audit)
    }

    /**
     * Load audits containing specified object
     */
    fun loadAudit(klass: KClass<*>, idToLoad: String): MutableList<MutableList<Any>> {
        val auditType = AuditType.resolveType(klass)
        val auditRecords = auditDao.loadRow(auditType, idToLoad)
        val resultList = ArrayList<MutableList<Any>>()

        for (auditRecord in auditRecords) {
            val currentRec = ArrayList<Any>()
            for (o in auditRecord.objects) {
                val (type, id) = o
                val objectRes = type.serializer.deserialize(id)
                currentRec.add(objectRes)
            }
            resultList.add(currentRec)
        }
        return resultList
    }

}