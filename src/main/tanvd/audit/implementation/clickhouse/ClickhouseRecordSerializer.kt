package tanvd.audit.implementation.clickhouse

import org.slf4j.LoggerFactory
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.getPredefinedAuditTableColumn
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.external.types.objects.StateType
import tanvd.audit.model.internal.AuditRecordInternal
import java.util.*
import kotlin.collections.HashSet

internal object ClickhouseRecordSerializer {

    private val logger = LoggerFactory.getLogger(ClickhouseRecordSerializer::class.java)

    /**
     * Serialize AuditRecordInternal for Clickhouse
     */
    fun serialize(auditRecordInternal: AuditRecordInternal): DbRow {

        val description = auditRecordInternal.objects.map { it.first.entityName }

        val elements = serializeObjects(auditRecordInternal)

        elements.add(DbColumn(getPredefinedAuditTableColumn(descriptionColumn), description))

        serializeInformation(auditRecordInternal, elements)

        return DbRow(elements)
    }

    private fun serializeObjects(auditRecordInternal: AuditRecordInternal): MutableList<DbColumn> {
        val groupedObjects = auditRecordInternal.objects.flatMap { it.second.stateList.entries }.groupBy { it.key }
                .mapValues { it.value.map { it.value } }
        val elements = groupedObjects.map { DbColumn(it.key.getCode(), it.value, it.key.toDbColumnType()) }.toMutableList()
        //Add mandatory columns
        return elements
    }

    private fun serializeInformation(auditRecordInternal: AuditRecordInternal, elements: MutableList<DbColumn>) {
        for (type in InformationType.getTypes()) {
            val curInformation = auditRecordInternal.information.find { it.type == type }
            if (curInformation != null) {
                elements.add(DbColumn(type.toDbColumnHeader(), type.presenter.serialize(curInformation.value)))
            } else {
                elements.add(DbColumn(type.toDbColumnHeader(), type.presenter.serialize(type.presenter.getDefault())))
            }
        }
    }


    /**
     * Deserialize AuditRecordInternal from string representation
     * DbString in Map -- name stateName
     *
     * @throws UnknownObjectTypeException
     */
    fun deserialize(row: DbRow): AuditRecordInternal {
        //mandatory columns
        val description = row.columns.find { it.name == descriptionColumn }
        if (description == null) {
            logger.error("Clickhouse scheme violated. Not found $descriptionColumn column.")
            return AuditRecordInternal(emptyList(), HashSet())
        }

        val information = deserializeInformation(row)

        val objects = deserializeObjects(description, row)

        return AuditRecordInternal(objects, information)
    }

    private fun deserializeObjects(description: DbColumn, row: DbRow): ArrayList<Pair<ObjectType<Any>, ObjectState>> {
        val objects = ArrayList<Pair<ObjectType<Any>, ObjectState>>()
        val currentNumberOfType = HashMap<String, Int>()

        for (code in description.elements) {
            val type = ObjectType.resolveType(code)
            val stateList = HashMap<StateType<*>, String>()
            for (stateType in type.state) {
                val pair = row.columns.find { it.name == stateType.getCode() }
                if (pair != null) {
                    val index = currentNumberOfType[stateType.getCode()] ?: 0
                    stateList.put(stateType, pair.elements[index])
                    currentNumberOfType.put(stateType.getCode(), index + 1)
                }
            }
            objects.add(Pair(type, ObjectState(stateList)))

        }
        return objects
    }

    private fun deserializeInformation(row: DbRow): HashSet<InformationObject<*>> {
        val information = HashSet<InformationObject<*>>()
        for (type in InformationType.getTypes()) {
            val curInformation = row.columns.find { it.name == type.code }
            if (curInformation != null) {
                information.add(InformationObject(type.deserialize(curInformation.elements[0]), type))
            }
        }
        return information
    }
}