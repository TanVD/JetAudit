package tanvd.audit.model.external.types.information

/**
 * External classes for users
 */
abstract class InformationLongPresenter: InformationPresenter<Long>()

abstract class InformationStringPresenter: InformationPresenter<String>()

abstract class InformationBooleanPresenter: InformationPresenter<Boolean>()

/**
 * Internal class
 */
abstract class InformationPresenter<T> {
    abstract val name: String
    abstract fun getDefault(): T

    override fun equals(other: Any?): Boolean {
        return (other is InformationPresenter<*>) && name == other.name
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}