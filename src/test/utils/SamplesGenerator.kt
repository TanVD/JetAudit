package utils

import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal

internal object SamplesGenerator {
    fun getRecordInternal(vararg obj: Any, information: MutableSet<InformationObject>) : AuditRecordInternal {
        val listObjects = ArrayList<Pair<ObjectType<*>, ObjectState>>()
        for (o in obj) {
            val type = ObjectType.resolveType(o::class)
            listObjects.add(type to type.serialize(o))
        }
        return AuditRecordInternal(listObjects, information)
    }
}