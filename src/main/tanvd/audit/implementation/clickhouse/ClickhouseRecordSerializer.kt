package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.getPredefinedAuditTableColumn
import tanvd.audit.implementation.clickhouse.model.DbColumn
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.implementation.clickhouse.model.toDbColumnHeader
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import java.util.*
import kotlin.collections.HashSet

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

        //Add mandatory columns
        elements.add(DbColumn(getPredefinedAuditTableColumn(descriptionColumn), description))

        for (type in InformationType.getTypes()) {
            val curInformation = auditRecordInternal.information.find { it.type == type }
            if (curInformation != null) {
                elements.add(DbColumn(type.toDbColumnHeader(), curInformation.value.toString()))
            }
        }

        return DbRow(elements)
    }


    /**
     * Deserialize AuditRecordInternal from string representation
     * DbString in Map -- name code
     *
     * @throws UnknownAuditTypeException
     */
    fun deserialize(row: DbRow): AuditRecordInternal {
        //mandatory columns
        val description = row.columns.find { it.name == descriptionColumn }
        if (description == null) {
            logger.error("Clickhouse scheme violated. Not found $descriptionColumn column.")
            return AuditRecordInternal(emptyList(), HashSet())
        }

        val information = HashSet<InformationObject>()

        for (type in InformationType.getTypes()) {
            val curInformation = row.columns.find { it.name == type.code }
            if (curInformation != null) {
                when (type.type) {
                    InformationType.InformationInnerType.Long -> {
                        information.add(InformationObject(curInformation.elements[0].toLong(), type))
                    }
                    InformationType.InformationInnerType.String -> {
                        information.add(InformationObject(curInformation.elements[0], type))
                    }
                    InformationType.InformationInnerType.Boolean -> {
                        information.add(InformationObject(curInformation.elements[0].toBoolean(), type))
                    }
                    InformationType.InformationInnerType.ULong -> {
                        information.add(InformationObject(curInformation.elements[0].toLong(), type))
                    }
                }
            }
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

        return AuditRecordInternal(objects, information)
    }
}