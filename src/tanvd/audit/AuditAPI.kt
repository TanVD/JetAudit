package tanvd.audit

import tanvd.audit.implementation.AuditDao
import tanvd.audit.implementation.AuditExecutor
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
 * in some other persistent context. Array [GIRL, "robbed by", FOX, "and", WOLF] will be saved as
 * "RedHat robbed by Lisa and Jack" (with some simplification) where ID's are retrieved with appropriate
 * saving function. After it you can load original array, all ID's will be resolved to objects with
 * appropriate loading function.
 *
 * Pay attention, that normal work of AuditAPI depends on external persistent context.
 */
class AuditAPI() {

    //Weave all classes
    companion object State {
        val saveFuncs: MutableMap<KClass<*>, (Any) -> String> = HashMap()

        val loadFuncs: MutableMap<KClass<*>, (String) -> Any> = HashMap()

        val auditQueue: LinkedBlockingQueue<AuditRecord> = LinkedBlockingQueue()

        val executor: AuditExecutor = AuditExecutor(auditQueue)

        val auditDao: AuditDao = AuditDao()

    }


    /**
     * Add types for audit saving
     */
    fun addTypeForAudit(klass: KClass<*>, saveFunc: (Any) -> String, loadFunc: (String) -> Any) {
        saveFuncs.put(klass, saveFunc)
        loadFuncs.put(klass, loadFunc)
        auditDao.addColumn(klass)
    }

    /**
     * Save audit entry resolving dependencies.
     * Unknown types will be ignored (for now).
     */
    fun saveAudit(vararg objects: Any) {
        val audit = AuditRecord()
        for (o in objects) {
            when (o) {
                is String -> {
                    audit.objects.add(Pair(String::class, o))
                }
                else -> {
                    val objectClass = o.javaClass.kotlin
                    val objectId = saveFuncs[objectClass]?.invoke(o)
                    if (objectId != null) {
                        audit.objects.add(Pair(objectClass, objectId))
                    }
                }
            }
        }

        auditQueue.put(audit)
    }

    /**
     * Load audits containing specified object
     */
    fun loadAudit(typeToLoad: KClass<*>, idToLoad: String): MutableList<MutableList<Any>> {
        val auditRecords = auditDao.loadRow(typeToLoad, idToLoad)
        val resultList = ArrayList<MutableList<Any>>()
        for (auditRecord in auditRecords) {
            val currentRec = ArrayList<Any>()
            for (o in auditRecord.objects) {
                val (type, id) = o
                if (type == String::class) {
                    currentRec.add(id)
                } else {
                    val objectRes = loadFuncs[type]?.invoke(id)
                    if (objectRes != null) {
                        currentRec.add(objectRes)
                    }
                }
            }
            resultList.add(currentRec)
        }
        return resultList
    }

}