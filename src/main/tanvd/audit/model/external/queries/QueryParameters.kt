package tanvd.audit.model.external.queries

import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order
import tanvd.audit.model.external.types.information.InformationPresenter
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.external.types.objects.StateType
import kotlin.reflect.KClass

/**
 * Query parameters for JetAudit
 */
class QueryParameters {
    val limits = LimitParameters()

    val orderBy = OrderByParameters()

    fun setLimits(start: Int, length: Int) {
        limits.isLimited = true
        limits.limitStart = start
        limits.limitLength = length
    }

    fun setInformationOrder(vararg information: Pair<InformationPresenter<*>, Order>) {
        orderBy.isOrdered = true
        orderBy.codesInformation = linkedMapOf(*information.map { Pair(InformationType.resolveType(it.first), it.second) }.toTypedArray())

    }

    fun setObjectStatesOrder(vararg states: Pair<StateType<*>, Order>) {
        orderBy.isOrdered = true
        orderBy.codesState = linkedMapOf(*states)
    }

    class LimitParameters {
        var isLimited = false
        var limitStart = 0
        var limitLength = 0
    }

    /**
     * First output will be ordered by objects, than by information in order of appearance.
     *
     * Beware, that comparators will be applied to arrays of value's of objects, not to objects itself
     */
    class OrderByParameters {
        var isOrdered = false
        var codesState = LinkedHashMap<StateType<*>, Order>()
        var codesInformation = LinkedHashMap<InformationType<*>, Order>()

        enum class Order {
            ASC,
            DESC
        }
    }
}
