package tanvd.audit.model.external.types.objects

import tanvd.audit.model.external.records.ObjectState

interface ObjectSerializer<T> {
    /**
     * If it can not find entity null instead will be returned
     */
    fun deserializeBatch(primaryKeys: List<ObjectState>): Map<ObjectState, T?> {
        val deserializedMap: MutableMap<ObjectState, T?> = HashMap()
        for (primaryKey in primaryKeys) {
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
