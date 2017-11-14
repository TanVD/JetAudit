package tanvd.audit.model.external.types.objects

import tanvd.aorm.DbBoolean
import tanvd.aorm.DbDate
import tanvd.aorm.DbLong
import tanvd.aorm.DbString
import tanvd.audit.model.external.records.ObjectState
import java.util.*

abstract class ObjectPresenter<T : Any> : ObjectSerializer<T> {
    abstract val entityName: String
    val fieldSerializers: MutableMap<StateType<*>, (T) -> Any> = HashMap()
    abstract val deserializer: (ObjectState) -> T?

    override fun deserialize(primaryKey: ObjectState): T? {
        return deserializer.invoke(primaryKey)
    }

    override fun serialize(entity: T): ObjectState {
        val stateList = HashMap<StateType<*>, Any>()
        for ((key, value) in fieldSerializers) {
            stateList.put(key, value.invoke(entity))
        }
        return ObjectState(stateList)
    }
}

fun <T : Any> ObjectPresenter<T>.long(name: String, body: (T) -> Long): StateType<Long> {
    val type = StateType(name, entityName, DbLong())
    fieldSerializers.put(type, body)
    return type
}

fun <T : Any> ObjectPresenter<T>.string(name: String, body: (T) -> String): StateType<String> {
    val type = StateType(name, entityName, DbString())
    fieldSerializers.put(type, body)
    return type
}

fun <T : Any> ObjectPresenter<T>.boolean(name: String, body: (T) -> Boolean): StateType<Boolean> {
    val type = StateType(name, entityName, DbBoolean())
    fieldSerializers.put(type, body)
    return type
}

fun <T : Any> ObjectPresenter<T>.date(name: String, body: (T) -> Date): StateType<Date> {
    val type = StateType(name, entityName, DbDate())
    fieldSerializers.put(type, body)
    return type
}
