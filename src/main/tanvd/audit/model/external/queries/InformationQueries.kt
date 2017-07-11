package tanvd.audit.model.external.queries

import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationBooleanPresenter
import tanvd.audit.model.external.types.information.InformationLongPresenter
import tanvd.audit.model.external.types.information.InformationPresenter
import tanvd.audit.model.external.types.information.InformationStringPresenter

sealed class QueryInformationLeafCondition<T>(val condition: InnerTypeCondition,
                                              val value: Any, val valueType: InnerType,
                                              val presenter: InformationPresenter<T>) : QueryExpression


//Equality interface
class QueryEqualityInformationLeaf<T : Any>(condition: EqualityCondition, value: T, valueType: InnerType,
                                            presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

//Implementations
infix fun InformationLongPresenter.equal(value: Long): QueryEqualityInformationLeaf<Long> {
    return QueryEqualityInformationLeaf(EqualityCondition.equal, value, InnerType.Long, this)
}

infix fun InformationLongPresenter.notEqual(value: Long): QueryEqualityInformationLeaf<Long> {
    return QueryEqualityInformationLeaf(EqualityCondition.notEqual, value, InnerType.Long, this)
}

infix fun InformationBooleanPresenter.equal(value: Boolean): QueryEqualityInformationLeaf<Boolean> {
    return QueryEqualityInformationLeaf(EqualityCondition.equal, value, InnerType.Boolean, this)
}

infix fun InformationBooleanPresenter.notEqual(value: Boolean): QueryEqualityInformationLeaf<Boolean> {
    return QueryEqualityInformationLeaf(EqualityCondition.notEqual, value, InnerType.Boolean, this)
}

infix fun InformationStringPresenter.equal(value: String): QueryEqualityInformationLeaf<String> {
    return QueryEqualityInformationLeaf(EqualityCondition.equal, value, InnerType.String, this)
}

infix fun InformationStringPresenter.notEqual(value: String): QueryEqualityInformationLeaf<String> {
    return QueryEqualityInformationLeaf(EqualityCondition.notEqual, value, InnerType.String, this)
}

//String interface
class QueryStringInformationLeaf<T : Any>(condition: StringCondition, value: T, valueType: InnerType,
                                          presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationStringPresenter.like(value: String): QueryStringInformationLeaf<String> {
    return QueryStringInformationLeaf(StringCondition.like, value, InnerType.String, this)
}

infix fun InformationStringPresenter.regexp(value: String): QueryStringInformationLeaf<String> {
    return QueryStringInformationLeaf(StringCondition.regexp, value, InnerType.String, this)
}

//Number interface
class QueryNumberInformationLeaf<T : Any>(condition: NumberCondition, value: T, valueType: InnerType,
                                          presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationLongPresenter.less(value: Long): QueryNumberInformationLeaf<Long> {
    return QueryNumberInformationLeaf(NumberCondition.less, value, InnerType.Long, this)
}

infix fun InformationLongPresenter.more(value: Long): QueryNumberInformationLeaf<Long> {
    return QueryNumberInformationLeaf(NumberCondition.more, value, InnerType.Long, this)
}

//List interface

class QueryListInformationLeaf<T : Any>(condition: ListCondition, value: List<T>, valueType: InnerType,
                                        presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationLongPresenter.inList(value: List<Long>): QueryListInformationLeaf<Long> {
    return QueryListInformationLeaf(ListCondition.inList, value, InnerType.Long, this)
}

infix fun InformationLongPresenter.notInList(value: List<Long>): QueryListInformationLeaf<Long> {
    return QueryListInformationLeaf(ListCondition.notInList, value, InnerType.Long, this)
}

infix fun InformationBooleanPresenter.inList(value: List<Boolean>): QueryListInformationLeaf<Boolean> {
    return QueryListInformationLeaf(ListCondition.inList, value, InnerType.Boolean, this)
}

infix fun InformationBooleanPresenter.notInList(value: List<Boolean>): QueryListInformationLeaf<Boolean> {
    return QueryListInformationLeaf(ListCondition.notInList, value, InnerType.Boolean, this)
}

infix fun InformationStringPresenter.inList(value: List<String>): QueryListInformationLeaf<String> {
    return QueryListInformationLeaf(ListCondition.inList, value, InnerType.String, this)
}

infix fun InformationStringPresenter.notInList(value: List<String>): QueryListInformationLeaf<String> {
    return QueryListInformationLeaf(ListCondition.notInList, value, InnerType.String, this)
}

