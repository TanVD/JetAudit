package tanvd.audit.model

import java.util.*

class AuditRecord(val objects: MutableList<Pair<AuditType<Any>, String>> = ArrayList()) {
    companion object Serialization {
        val delimiter = '\u0001'

        /**
         * Serialize AuditRecord to String representation.
         * String serialized as is, types serialized as
         * [delimiter]type fully qualified name [delimiter] string id representation [delimiter]
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
         * Deserialize AuditRecord from string representation
         */
        fun deserialize(auditStringParam: String): AuditRecord {
            var auditString = auditStringParam
            val auditRecord = AuditRecord()
            while (auditString.isNotEmpty()) {
                val code = auditString.subSequence(1, auditString.indexOf(delimiter, 1))
                auditString = auditString.drop(auditString.indexOf(delimiter, 1))
                val id = auditString.subSequence(1, auditString.indexOf(delimiter, 1)).toString()
                auditString = auditString.drop(auditString.indexOf(delimiter, 1) + 1)
                val type: AuditType<Any>? = AuditType.resolveType(code.toString())
                if (type != null) {
                    auditRecord.objects.add(Pair(type, id))
                }
            }

            return auditRecord
        }
    }

}