package tanvd.audit.model.external.types.information

import tanvd.audit.implementation.clickhouse.model.getDateFormat
import java.util.*

/**
 * External classes for users
 */
abstract class InformationLongPresenter : InformationPresenter<Long>() {
    override fun deserialize(serialized: String): Long {
        return serialized.toLong()
    }

    override fun serialize(entity: Long): String {
        return entity.toString()
    }
}

abstract class InformationStringPresenter : InformationPresenter<String>() {
    override fun deserialize(serialized: String): String {
        return serialized
    }

    override fun serialize(entity: String): String {
        return entity
    }
}

abstract class InformationBooleanPresenter : InformationPresenter<Boolean>() {
    override fun deserialize(serialized: String): Boolean {
        return serialized.toBoolean()
    }

    override fun serialize(entity: Boolean): String {
        return entity.toString()
    }
}

abstract class InformationDatePresenter : InformationPresenter<Date>() {
    override fun deserialize(serialized: String): Date {
        return getDateFormat().parse(serialized)
    }

    override fun serialize(entity: Date): String {
        return getDateFormat().format(entity)
    }
}

/**
 * Internal class
 */
sealed class InformationPresenter<T : Any> : InformationSerializer<T> {
    abstract val code: String
    abstract fun getDefault(): T

    override fun equals(other: Any?): Boolean {
        return (other is InformationPresenter<*>) && code == other.code
    }

    override fun hashCode(): Int {
        return code.hashCode()
    }
}