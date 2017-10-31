package tanvd.audit.model.external.types.objects

import tanvd.audit.model.external.records.ObjectState

interface ObjectSerializer<T> {

    /**
     * You can set false to useDeserialization field in case if you do not want to use
     * deserialization. AuditObjects will have null instead of object. State will be returned.
     */
    val useDeserialization: Boolean

    /**
     * If it can not find entity null instead will be returned
     */
    fun deserializeBatch(states: List<ObjectState>): Map<ObjectState, T?> {
        val deserializedMap: MutableMap<ObjectState, T?> = HashMap()
        for (primaryKey in states) {
            deserializedMap.put(primaryKey, deserialize(primaryKey))
        }
        return deserializedMap
    }

    /**
     * If it can not find entity null instead will be returned
     */
    fun deserialize(primaryKey: ObjectState): T?

    fun serialize(entity: T): ObjectState
}
