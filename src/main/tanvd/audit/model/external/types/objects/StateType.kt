package tanvd.audit.model.external.types.objects

import tanvd.aorm.*
import java.util.*

class StateType<T : Any>(val stateName: String, val objectName: String, type: DbPrimitiveType<T>) {
    internal val type: DbArrayType<T> = type.toArray()
    internal val column : Column<List<T>, DbArrayType<T>> = Column("${objectName}_$stateName", this.type)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other?.javaClass != javaClass) return false

        other as StateType<*>

        if (stateName != other.stateName) return false
        if (objectName != other.objectName) return false
        if (type != other.type) return false

        return true
    }

    override fun hashCode(): Int {
        var result = stateName.hashCode()
        result = 31 * result + objectName.hashCode()
        result = 31 * result + type.hashCode()
        return result
    }
}

//Helper functions

fun <T : Any>ObjectPresenter<T>.date(name: String) : StateType<Date> {
    return StateType(name, entityName, DbDate())
}

fun <T : Any>ObjectPresenter<T>.long(name: String) : StateType<Long> {
    return StateType(name, entityName, DbLong())
}

fun <T : Any>ObjectPresenter<T>.string(name: String) : StateType<String> {
    return StateType(name, entityName, DbString())
}