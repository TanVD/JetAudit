package tanvd.audit.implementation.clickhouse

import tanvd.audit.implementation.clickhouse.model.DbColumn
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import java.util.*

object ClickhouseRecordSerializer {
    val delimiter = '\u0001'

    /**
     * Serialize AuditRecord for Clickhouse
     * Serialize order to string of types
     * Serialize objects to groups by types respecting order
     */
    fun serialize(auditRecord: AuditRecord): Pair<String, DbRow> {
        val serializedAudit = StringBuilder()
        for (o in auditRecord.objects) {
            serializedAudit.append(delimiter)
            serializedAudit.append(o.first.code)
            serializedAudit.append(delimiter)
        }
        val groupedObjects = auditRecord.objects.groupBy { it.first }.mapValues {
            entry -> entry.value.map {it.second}
        }

        val row = DbRow(groupedObjects.map { DbColumn(it.key.code, it.value, DbColumnType.DbArrayString) }.toMutableList())

        return Pair(serializedAudit.toString(), row)
    }

    /**
     * Deserialize MySQL.AuditRecord from string representation
     * DbString in Map -- name code
     */
    fun deserialize(row : DbRow): AuditRecord {
        var auditString = row.columns.find { it.name == "description" }!!.elements[0]
        val auditRecord = AuditRecord()
        val currentNumberOfType = HashMap<String, Int>()

        while (auditString.isNotEmpty()) {
            val code = auditString.subSequence(1, auditString.indexOf(delimiter, 1)).toString()
            auditString = auditString.drop(auditString.indexOf(delimiter, 1) + 1)

            val pair = row.columns.find { it.name == code }
            if (pair != null) {
                var index = currentNumberOfType[code]
                if (index == null) {
                    index = 0
                }
                if (pair.elements[index].isNotEmpty()) {
                    auditRecord.objects.add(Pair(AuditType.resolveType(code), pair.elements[index]))
                }
                currentNumberOfType.put(code, index + 1)
            }
        }

        return auditRecord
    }
}