package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.getPredefinedAuditTableColumn
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.unixTimeStampColumn
import tanvd.audit.implementation.clickhouse.model.DbColumn
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.internal.AuditRecordInternal
import java.util.*

internal object ClickhouseRecordSerializer {

    private val logger = LoggerFactory.getLogger(ClickhouseRecordSerializer::class.java)

    /**
     * Serialize AuditRecordInternal for Clickhouse
     */
    fun serialize(auditRecordInternal: AuditRecordInternal): DbRow {

        val description = auditRecordInternal.objects.map { it.first.code }

        val groupedObjects = auditRecordInternal.objects.groupBy { it.first }.mapValues {
            entry ->
            entry.value.map { it.second }
        }

        val elements = groupedObjects.map { DbColumn(it.key.code, it.value, DbColumnType.DbArrayString) }.toMutableList()
        elements.add(DbColumn(getPredefinedAuditTableColumn(descriptionColumn), description))
        elements.add(DbColumn(getPredefinedAuditTableColumn(unixTimeStampColumn), auditRecordInternal.unixTimeStamp.toString()))

        return DbRow(elements)
    }

    /**
     * Deserialize AuditRecordInternal from string representation
     * DbString in Map -- name code
     *
     * @throws UnknownAuditTypeException
     */
    fun deserialize(row: DbRow): AuditRecordInternal {
        val description = row.columns.find { it.name == descriptionColumn }
        if (description == null) {
            logger.error("Clickhouse scheme violated. Not found $descriptionColumn column.")
            return AuditRecordInternal(emptyList(), 0)
        }
        val unixTimeStamp = row.columns.find { it.name == unixTimeStampColumn }
        if (unixTimeStamp == null) {
            logger.error("Clickhouse scheme violated. Not found $unixTimeStampColumn column.")
            return AuditRecordInternal(emptyList(), 0)
        }
        val objects = ArrayList<Pair<AuditType<Any>, String>>()
        val currentNumberOfType = HashMap<String, Int>()

        for (code in description.elements) {
            val pair = row.columns.find { it.name == code }
            if (pair != null) {
                val index = currentNumberOfType[code] ?: 0
                if (pair.elements[index].isNotEmpty()) {
                    val type = AuditType.resolveType(code)
                    objects.add(Pair(type, pair.elements[index]))
                }
                currentNumberOfType.put(code, index + 1)
            }
        }

        return AuditRecordInternal(objects, unixTimeStamp.elements[0].toLong())
    }
}