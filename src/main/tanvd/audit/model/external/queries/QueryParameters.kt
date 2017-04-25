package tanvd.audit.model.external.queries

import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.InformationPresenter
import tanvd.audit.model.external.types.InformationType
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

    @Throws(UnknownAuditTypeException::class)
    fun setOrder(vararg klasses: Pair<KClass<*>, Order>, information: List<Pair<InformationPresenter<*>, Order>>) {
        orderBy.isOrdered = true
        orderBy.codes = arrayListOf(*klasses.map { Pair(AuditType.resolveType(it.first).code, it.second) }.toTypedArray())
        orderBy.codes.addAll(information.map { Pair(InformationType.resolveType(it.first).code, it.second) })
    }

    class LimitParameters {
        var isLimited = false
        var limitStart = 0
        var limitLength = 0
    }

    /**
     * First output will be ordered by TimeStamp, than by types in order of appearance.
     *
     * Beware, that comparators will be applied to arrays of id's of objects, not to objects itself
     */
    class OrderByParameters {
        var isOrdered = false
        var codes = ArrayList<Pair<String, Order>>()

        enum class Order {
            ASC,
            DESC
        }
    }
}
