package tanvd.audit.model.external.types.objects

import tanvd.audit.model.external.records.ObjectState

abstract class ObjectPresenter<T> : ObjectSerializer<T> {
    abstract val entityName: String
    abstract val fieldSerializers: Map<StateType<T>, (T) -> String>
    abstract val deserializer: (ObjectState) -> T?

    override fun deserialize(primaryKey: ObjectState): T? {
        return deserializer.invoke(primaryKey)
    }

    override fun serialize(entity: T): ObjectState {
        val stateList = HashMap<StateType<T>, String>()
        for ((key, value) in fieldSerializers) {
            stateList.put(key, value.invoke(entity))
        }
        @Suppress("UNCHECKED_CAST")
        return ObjectState(stateList as Map<StateType<*>, String>)
    }
}
