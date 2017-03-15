package tanvd.audit

import java.util.*
import kotlin.reflect.KClass

class AuditRecord(val objects : MutableList<Pair<KClass<*>, String>> = ArrayList()) {
    companion object Serialization {
        val delimiter = '\u0001'

        /**
         * Serialize AuditRecord to String representation.
         * String serialized as is, types serialized as
         * [delimiter]type fully qualified name [delimiter] string id representation [delimiter]
         */
        fun serialize(auditRecord: AuditRecord) : String {
            val serializedAudit = StringBuilder()
            for (o in auditRecord.objects) {
                if (o.first == String::class) {
                    serializedAudit.append(o.second)
                }
                else {
                    serializedAudit.append(delimiter)
                    serializedAudit.append(o.first.qualifiedName)
                    serializedAudit.append(delimiter)
                    serializedAudit.append(o.second)
                    serializedAudit.append(delimiter)
                }
            }
            return serializedAudit.toString();
        }

        /**
         * Deserialize AuditRecord from string representation
         */
        fun deserialize(auditStringParam : String, types : List<KClass<*>>) : AuditRecord {
            var auditString = auditStringParam
            val auditRecord = AuditRecord()
            while (auditString.isNotEmpty()) {
                val indexOfDelimiter = auditString.indexOf(delimiter)
                if (indexOfDelimiter == -1){
                    auditRecord.objects.add(Pair(String::class, auditString))
                    break
                }
                else if (indexOfDelimiter == 0) {
                    val qualifiedNamed = auditString.subSequence(1, auditString.indexOf(delimiter, 1))
                    auditString = auditString.drop(auditString.indexOf(delimiter, 1))
                    val id = auditString.subSequence(1, auditString.indexOf(delimiter, 1)).toString()
                    auditString = auditString.drop(auditString.indexOf(delimiter, 1) + 1)
                    val type : KClass<*>? = types.find { it.qualifiedName == qualifiedNamed }
                    if (type != null) {
                        auditRecord.objects.add(Pair(type, id));
                    }
                }
                else {
                    val string = auditString.subSequence(0, auditString.indexOf(delimiter)).toString()
                    auditString = auditString.drop(auditString.indexOf(delimiter))
                    auditRecord.objects.add(Pair(String::class, string))
                }
            }

            return auditRecord
        }
    }

}