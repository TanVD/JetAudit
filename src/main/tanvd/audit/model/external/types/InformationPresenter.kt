package tanvd.audit.model.external.types

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

