package tanvd.audit.implementation.clickhouse

import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Config.auditDescriptionColumnName
import tanvd.audit.implementation.clickhouse.model.DbColumn
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import java.util.*

object ClickhouseRecordSerializer {
    /**
     * Serialize AuditRecord for Clickhouse
     * Serialize order to string of types
     * Serialize objects to groups by types respecting order
     */
    fun serialize(auditRecord: AuditRecord): DbRow {

        val description = auditRecord.objects.map { it.first.code }

        val groupedObjects = auditRecord.objects.groupBy { it.first }.mapValues {
            entry -> entry.value.map {it.second}
        }


        val row = DbRow(groupedObjects.map { DbColumn(it.key.code, it.value, DbColumnType.DbArrayString) }.toMutableList())
        row.columns.add(DbColumn(auditDescriptionColumnName, description, DbColumnType.DbArrayString))

        return row
    }

    /**
     * Deserialize MySQL.AuditRecord from string representation
     * DbString in Map -- name code
     */
    fun deserialize(row : DbRow): AuditRecord {
        val description = row.columns.find { it.name == auditDescriptionColumnName }!!
        val auditRecord = AuditRecord()
        val currentNumberOfType = HashMap<String, Int>()

       for (code in description.elements) {
            val pair = row.columns.find { it.name == code }
            if (pair != null) {
                val index = currentNumberOfType[code] ?: 0
                if (pair.elements[index].isNotEmpty()) {
                    auditRecord.objects.add(Pair(AuditType.resolveType(code), pair.elements[index]))
                }
                currentNumberOfType.put(code, index + 1)
            }
        }

        return auditRecord
    }
}