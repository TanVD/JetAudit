package tanvd.audit.model.external.queries

import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.objects.StateBooleanType
import tanvd.audit.model.external.types.objects.StateLongType
import tanvd.audit.model.external.types.objects.StateStringType
import tanvd.audit.model.external.types.objects.StateType

sealed class QueryTypeLeafCondition<T>(val condition: InnerTypeCondition, val value: Any,
                                       val valueType: InnerType, val stateType: StateType<T>) : QueryExpression

//Equality interface
class QueryEqualityTypeLeaf<T : Any>(condition: EqualityCondition, value: T, valueType: InnerType,
                                     stateType: StateType<T>) :
        QueryTypeLeafCondition<T>(condition, value, valueType, stateType)

//Implementations
infix fun StateLongType.equal(number: Long): QueryEqualityTypeLeaf<Long> {
    return QueryEqualityTypeLeaf(EqualityCondition.equal, number, InnerType.Long, this)
}

infix fun StateLongType.notEqual(number: Long): QueryEqualityTypeLeaf<Long> {
    return QueryEqualityTypeLeaf(EqualityCondition.notEqual, number, InnerType.Long, this)
}

infix fun StateBooleanType.equal(number: Boolean): QueryEqualityTypeLeaf<Boolean> {
    return QueryEqualityTypeLeaf(EqualityCondition.equal, number, InnerType.Boolean, this)
}

infix fun StateBooleanType.notEqual(number: Boolean): QueryEqualityTypeLeaf<Boolean> {
    return QueryEqualityTypeLeaf(EqualityCondition.notEqual, number, InnerType.Boolean, this)
}

infix fun StateStringType.equal(number: String): QueryEqualityTypeLeaf<String> {
    return QueryEqualityTypeLeaf(EqualityCondition.equal, number, InnerType.String, this)
}

infix fun StateStringType.notEqual(number: String): QueryEqualityTypeLeaf<String> {
    return QueryEqualityTypeLeaf(EqualityCondition.notEqual, number, InnerType.String, this)
}

//String interface
class QueryStringTypeLeaf<T : Any>(condition: StringCondition, value: T, valueType: InnerType,
                                   stateType: StateType<T>) :
        QueryTypeLeafCondition<T>(condition, value, valueType, stateType)

//Implementations
infix fun StateStringType.like(value: String): QueryStringTypeLeaf<String> {
    return QueryStringTypeLeaf(StringCondition.like, value, InnerType.String, this)
}

infix fun StateStringType.regexp(value: String): QueryStringTypeLeaf<String> {
    return QueryStringTypeLeaf(StringCondition.regexp, value, InnerType.String, this)
}

//Number interface
class QueryNumberTypeLeaf<T : Any>(condition: NumberCondition, value: T, valueType: InnerType,
                                   stateType: StateType<T>) :
        QueryTypeLeafCondition<T>(condition, value, valueType, stateType)

//Implementations
infix fun StateLongType.less(value: Long): QueryNumberTypeLeaf<Long> {
    return QueryNumberTypeLeaf(NumberCondition.less, value, InnerType.Long, this)
}

infix fun StateLongType.more(value: Long): QueryNumberTypeLeaf<Long> {
    return QueryNumberTypeLeaf(NumberCondition.more, value, InnerType.Long, this)
}

//List interface
class QueryListTypeLeaf<T : Any>(condition: ListCondition, value: List<T>, valueType: InnerType,
                                 stateType: StateType<T>) :
        QueryTypeLeafCondition<T>(condition, value, valueType, stateType)

//Implementations
infix fun StateLongType.inList(value: List<Long>): QueryListTypeLeaf<Long> {
    return QueryListTypeLeaf(ListCondition.inList, value, InnerType.Long, this)
}

infix fun StateLongType.notInList(number: List<Long>): QueryListTypeLeaf<Long> {
    return QueryListTypeLeaf(ListCondition.notInList, number, InnerType.Long, this)
}

infix fun StateBooleanType.inList(number: List<Boolean>): QueryListTypeLeaf<Boolean> {
    return QueryListTypeLeaf(ListCondition.inList, number, InnerType.Boolean, this)
}

infix fun StateBooleanType.notInList(number: List<Boolean>): QueryListTypeLeaf<Boolean> {
    return QueryListTypeLeaf(ListCondition.notInList, number, InnerType.Boolean, this)
}

infix fun StateStringType.inList(number: List<String>): QueryListTypeLeaf<String> {
    return QueryListTypeLeaf(ListCondition.inList, number, InnerType.String, this)
}

infix fun StateStringType.notInList(number: List<String>): QueryListTypeLeaf<String> {
    return QueryListTypeLeaf(ListCondition.notInList, number, InnerType.String, this)
}