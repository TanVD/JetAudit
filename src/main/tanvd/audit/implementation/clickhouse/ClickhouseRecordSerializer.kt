package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import tanvd.aorm.Column
import tanvd.aorm.DbType
import tanvd.aorm.Row
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.external.types.objects.StateType
import tanvd.audit.model.internal.AuditRecordInternal
import java.util.*
import kotlin.collections.LinkedHashSet

internal object ClickhouseRecordSerializer {

    private val logger = LoggerFactory.getLogger(ClickhouseRecordSerializer::class.java)

    /**
     * Serialize AuditRecordInternal for Clickhouse
     */
    fun serialize(auditRecordInternal: AuditRecordInternal): Map<Column<*, DbType<*>>, Any> {
        val description = auditRecordInternal.objects.map { it.first.entityName }

        val elements = serializeObjects(auditRecordInternal)

        auditRecordInternal.information.forEach {
            elements.put(it.type.column, it.value)
        }

        elements.put(AuditTable.description, description)

        return elements
    }

    private fun serializeObjects(auditRecordInternal: AuditRecordInternal): MutableMap<Column<*, DbType<*>>, Any> {
        val groupedObjects = auditRecordInternal.objects.flatMap { it.second.stateList.entries }.groupBy { it.key }
                .mapValues { it.value.map { it.value } }
        return groupedObjects.mapKeys { it.key.column }.toMutableMap()
    }

    fun deserialize(row: Row): AuditRecordInternal {
        val description = row[AuditTable.description]
        if (description == null) {
            logger.error("Clickhouse scheme violated. Not found ${AuditTable.description.name} column.")
            return AuditRecordInternal(emptyList(), LinkedHashSet())
        }

        return AuditRecordInternal(deserializeObjects(description, row), deserializeInformation(row))
    }

    @Suppress("UNCHECKED_CAST")
    private fun deserializeObjects(description: List<String>, row: Row): ArrayList<Pair<ObjectType<Any>, ObjectState>> {
        val objects = ArrayList<Pair<ObjectType<Any>, ObjectState>>()
        val currentNumberOfType = HashMap<String, Int>()

        for (code in description) {
            val type = ObjectType.resolveType(code)
            val stateList = HashMap<StateType<*>, Any>()
            for (stateType in type.state) {
                val value = row[stateType.column.name] as List<Any>?
                if (value != null) {
                    val index = currentNumberOfType[stateType.column.name] ?: 0
                    stateList.put(stateType, value[index])
                    currentNumberOfType.put(stateType.column.name, index + 1)
                }
            }
            objects.add(Pair(type, ObjectState(stateList)))
        }
        return objects
    }

    private fun deserializeInformation(row: Row): LinkedHashSet<InformationObject<*>> {
        val information = LinkedHashSet<InformationObject<*>>()
        for (type in InformationType.getTypes()) {
            val curInformation = row[type.column]
            if (curInformation != null) {
                information.add(InformationObject(curInformation, type))
            }
        }
        return information
    }
}