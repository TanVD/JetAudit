package tanvd.audit.model.external.types.objects

import tanvd.audit.model.external.types.InnerType

/**
 * External class for users
 */
class StateLongType<T>(stateName: String, objectName: String) : StateType<T>(stateName, objectName, InnerType.Long)

class StateBooleanType<T>(stateName: String, objectName: String) : StateType<T>(stateName, objectName, InnerType.Boolean)

class StateStringType<T>(stateName: String, objectName: String) : StateType<T>(stateName, objectName, InnerType.String)


/**
 * Internal class
 */
abstract class StateType<T>(val stateName: String, val objectName: String, val type: InnerType) {
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