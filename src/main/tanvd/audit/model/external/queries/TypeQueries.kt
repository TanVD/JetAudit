package tanvd.audit.model.external.queries

import tanvd.audit.exceptions.UnknownAuditTypeException
import tanvd.audit.model.external.types.AuditType
import kotlin.reflect.KClass

//QueryTypeLeaf

/**
 *  Operators permitted to use with AuditTypes.
 */
enum class QueryTypeCondition {
    equal,
    like
}

/**
 * Leaf with AuditType.
 */
class QueryTypeLeaf(val typeCondition: QueryTypeCondition, val id: String, klass: KClass<*>) : QueryExpression {
    val type: AuditType<Any> = AuditType.resolveType(klass)
}

/**
 * Infix functions to construct readable expressions.
 */

@Throws(UnknownAuditTypeException::class)
infix fun <T : Any> KClass<T>.equal(obj: T): QueryTypeLeaf {
    return QueryTypeLeaf(QueryTypeCondition.equal, AuditType.resolveType(this).serialize(obj), this)
}

@Throws(UnknownAuditTypeException::class)
infix fun <T : Any> KClass<T>.like(regExp: String): QueryTypeLeaf {
    return QueryTypeLeaf(QueryTypeCondition.like, AuditType.resolveType(this).serialize(regExp), this)
}


