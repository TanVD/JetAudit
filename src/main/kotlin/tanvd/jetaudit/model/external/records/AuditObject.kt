package tanvd.jetaudit.model.external.records

import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.external.types.objects.StateType

/**
 * External representation of state of AuditObject
 */
data class ObjectState(val stateList: Map<StateType<*>, Any>) {
    operator fun get(stateType: StateType<*>): Any? = stateList[stateType]
}

/**
 * External representation of loaded audit object with rich type information.
 */
data class AuditObject(val type: ObjectType<*>, val obj: Any?, val state: ObjectState)