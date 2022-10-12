package tanvd.jetaudit.implementation.clickhouse

import org.slf4j.LoggerFactory
import tanvd.aorm.DbType
import tanvd.aorm.SelectRow
import tanvd.aorm.expression.Column
import tanvd.jetaudit.implementation.clickhouse.aorm.AuditTable
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.records.ObjectState
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.external.types.objects.StateType
import tanvd.jetaudit.model.internal.AuditRecordInternal
import java.util.*
import kotlin.collections.LinkedHashSet

internal object ClickhouseRecordSerializer {

    private val logger = LoggerFactory.getLogger(ClickhouseRecordSerializer::class.java)

    /**
     * Serialize AuditRecordInternal for Clickhouse
     */
    fun serialize(auditRecordInternal: AuditRecordInternal): Map<Column<Any, DbType<Any>>, Any> {
        val description = auditRecordInternal.objects.map { it.first.entityName }

        val elements = serializeObjects(auditRecordInternal)

        auditRecordInternal.information.forEach {
            elements[it.type.column] = it.value
        }

        elements[AuditTable.description] = description

        @Suppress("UNCHECKED_CAST")
        return elements as Map<Column<Any, DbType<Any>>, Any>
    }

    private fun serializeObjects(auditRecordInternal: AuditRecordInternal): MutableMap<Column<*, DbType<*>>, Any> {
        return auditRecordInternal.objects.flatMap {
            it.second.stateList.entries
        }.groupByTo(mutableMapOf(), { it.key.column }) { it.value } as MutableMap<Column<*, DbType<*>>, Any>
    }

    fun deserialize(row: SelectRow): AuditRecordInternal {
        val description = row[AuditTable.description]

        return AuditRecordInternal(deserializeObjects(description, row), deserializeInformation(row))
    }

    @Suppress("UNCHECKED_CAST")
    private fun deserializeObjects(description: List<String>, row: SelectRow): ArrayList<Pair<ObjectType<Any>, ObjectState>> {
        val objects = ArrayList<Pair<ObjectType<Any>, ObjectState>>()
        val currentNumberOfType = HashMap<String, Int>()

        for (code in description) {
            val type = ObjectType.resolveType(code)
            val stateList = HashMap<StateType<*>, Any>()
            for (stateType in type.state) {
                val value = row[stateType.column as Column<Any, DbType<Any>>] as List<Any>?
                if (value != null) {
                    val index = currentNumberOfType[stateType.column.name] ?: 0
                    stateList[stateType] = value[index]
                    currentNumberOfType[stateType.column.name] = index + 1
                }
            }
            objects.add(Pair(type, ObjectState(stateList)))
        }
        return objects
    }

    private fun deserializeInformation(row: SelectRow): LinkedHashSet<InformationObject<*>> {
        val information = LinkedHashSet<InformationObject<*>>()
        for (type in InformationType.getTypes()) {
            val curInformation = row[type.column]
            information.add(InformationObject(curInformation, type))
        }
        return information
    }
}