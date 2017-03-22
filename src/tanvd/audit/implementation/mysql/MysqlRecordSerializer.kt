package tanvd.audit.implementation.mysql

import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import java.util.*

object MysqlRecordSerializer {
    val delimiter = '\u0001'

    /**
     * Serialize MySQL.AuditRecord to String representation.
     * DbString serialized as is, types serialized as
     * [delimiter]name fully qualified name [delimiter] string id representation [delimiter]
     */
    fun serialize(auditRecord: AuditRecord): String {
        val serializedAudit = StringBuilder()
        for (o in auditRecord.objects) {
            serializedAudit.append(delimiter)
            serializedAudit.append(o.first.code)
            serializedAudit.append(delimiter)
            serializedAudit.append(o.second)
            serializedAudit.append(delimiter)
        }
        return serializedAudit.toString()
    }

    /**
     * Deserialize MySQL.AuditRecord from string representation
     */
    fun deserialize(auditStringParam: String): AuditRecord {
        var auditString = auditStringParam
        val objects = ArrayList<Pair<AuditType<Any>, String>>()
        while (auditString.isNotEmpty()) {
            val code = auditString.subSequence(1, auditString.indexOf(delimiter, 1))
            auditString = auditString.drop(auditString.indexOf(delimiter, 1))
            val id = auditString.subSequence(1, auditString.indexOf(delimiter, 1)).toString()
            auditString = auditString.drop(auditString.indexOf(delimiter, 1) + 1)
            val type: AuditType<Any>? = AuditType.resolveType(code.toString())
            if (type != null) {
                objects.add(Pair(type, id))
            }
        }

        return AuditRecord(objects)
    }
}