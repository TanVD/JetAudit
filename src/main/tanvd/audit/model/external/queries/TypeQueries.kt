package tanvd.audit.model.external.queries

import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.model.external.types.information.InformationLongPresenter
import tanvd.audit.model.external.types.objects.*
import javax.management.Query
import kotlin.reflect.KClass



/**
 * Leaf with LongStateType.
 */
class QueryTypeLongLeaf(val typeCondition: QueryLongCondition, val id: String,
                          val stateType: StateLongType<*> ) : QueryExpression


infix fun <T : Any> StateLongType<T>.less(number: Long): QueryTypeLongLeaf {
    return QueryTypeLongLeaf(QueryLongCondition.less, number.toString(), this)
}

infix fun <T : Any> StateLongType<T>.more(number: Long): QueryTypeLongLeaf {
    return QueryTypeLongLeaf(QueryLongCondition.more, number.toString(), this)
}

infix fun <T : Any> StateLongType<T>.equal(number: Long): QueryTypeLongLeaf {
    return QueryTypeLongLeaf(QueryLongCondition.equal, number.toString(), this)
}

/**
 * Leaf with StringStateType.
 */
class QueryTypeStringLeaf(val typeCondition: QueryStringCondition, val value: String,
                          val stateType: StateStringType<*> ) : QueryExpression


infix fun <T : Any> StateStringType<T>.like(string: String): QueryTypeStringLeaf {
    return QueryTypeStringLeaf(QueryStringCondition.like, string, this)
}

infix fun <T : Any> StateStringType<T>.equal(string: String): QueryTypeStringLeaf {
    return QueryTypeStringLeaf(QueryStringCondition.equal, string, this)
}

infix fun <T : Any> StateStringType<T>.regexp(string: String): QueryTypeStringLeaf {
    return QueryTypeStringLeaf(QueryStringCondition.regexp, string, this)
}

/**
 * Leaf with BooleanStateType.
 */
class QueryTypeBooleanLeaf(val typeCondition: QueryBooleanCondition, val id: String,
                          val stateType: StateBooleanType<*> ) : QueryExpression


infix fun <T : Any> StateBooleanType<T>.`is`(value: Boolean): QueryTypeBooleanLeaf {
    return QueryTypeBooleanLeaf(QueryBooleanCondition.`is`, value.toString(), this)
}

infix fun <T : Any> StateBooleanType<T>.isNot(value: Boolean): QueryTypeBooleanLeaf {
    return QueryTypeBooleanLeaf(QueryBooleanCondition.isNot, value.toString(), this)
}
