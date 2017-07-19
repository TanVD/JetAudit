package tanvd.audit.model.external.queries

import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.*
import java.util.*

sealed class QueryInformationLeafCondition<T : Any>(val condition: InnerTypeCondition,
                                                    val value: Any, val valueType: InnerType,
                                                    val presenter: InformationPresenter<T>) : QueryExpression


//Equality interface: Long, Date, Boolean, String
class QueryEqualityInformationLeaf<T : Any>(condition: EqualityCondition, value: T, valueType: InnerType,
                                            presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationLongPresenter.equal(value: Long): QueryEqualityInformationLeaf<Long> {
    return QueryEqualityInformationLeaf(EqualityCondition.equal, value, InnerType.Long, this)
}

infix fun InformationDatePresenter.equal(value: Date): QueryEqualityInformationLeaf<Date> {
    return QueryEqualityInformationLeaf(EqualityCondition.equal, value, InnerType.Date, this)
}

infix fun InformationBooleanPresenter.equal(value: Boolean): QueryEqualityInformationLeaf<Boolean> {
    return QueryEqualityInformationLeaf(EqualityCondition.equal, value, InnerType.Boolean, this)
}

infix fun InformationStringPresenter.equal(value: String): QueryEqualityInformationLeaf<String> {
    return QueryEqualityInformationLeaf(EqualityCondition.equal, value, InnerType.String, this)
}

//String interface: String
class QueryStringInformationLeaf<T : Any>(condition: StringCondition, value: T, valueType: InnerType,
                                          presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationStringPresenter.like(value: String): QueryStringInformationLeaf<String> {
    return QueryStringInformationLeaf(StringCondition.like, value, InnerType.String, this)
}

infix fun InformationStringPresenter.regexp(value: String): QueryStringInformationLeaf<String> {
    return QueryStringInformationLeaf(StringCondition.regexp, value, InnerType.String, this)
}

//Number interface: Long
class QueryNumberInformationLeaf<T : Any>(condition: NumberCondition, value: T, valueType: InnerType,
                                          presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationLongPresenter.less(value: Long): QueryNumberInformationLeaf<Long> {
    return QueryNumberInformationLeaf(NumberCondition.less, value, InnerType.Long, this)
}

infix fun InformationLongPresenter.more(value: Long): QueryNumberInformationLeaf<Long> {
    return QueryNumberInformationLeaf(NumberCondition.more, value, InnerType.Long, this)
}

//Date interface: Date
class QueryDateInformationLeaf<T : Any>(condition: DateCondition, value: T, valueType: InnerType,
                                        presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationDatePresenter.less(value: Date): QueryDateInformationLeaf<Date> {
    return QueryDateInformationLeaf(DateCondition.less, value, InnerType.Date, this)
}

infix fun InformationDatePresenter.lessOrEqual(value: Date): QueryDateInformationLeaf<Date> {
    return QueryDateInformationLeaf(DateCondition.lessOrEqual, value, InnerType.Date, this)
}

infix fun InformationDatePresenter.more(value: Date): QueryDateInformationLeaf<Date> {
    return QueryDateInformationLeaf(DateCondition.more, value, InnerType.Date, this)
}

infix fun InformationDatePresenter.moreOrEqual(value: Date): QueryDateInformationLeaf<Date> {
    return QueryDateInformationLeaf(DateCondition.moreOrEqual, value, InnerType.Date, this)
}

//List interface: Long, Date, Boolean, String

class QueryListInformationLeaf<T : Any>(condition: ListCondition, value: List<T>, valueType: InnerType,
                                        presenter: InformationPresenter<T>) :
        QueryInformationLeafCondition<T>(condition, value, valueType, presenter)

infix fun InformationLongPresenter.inList(value: List<Long>): QueryListInformationLeaf<Long> {
    return QueryListInformationLeaf(ListCondition.inList, value, InnerType.Long, this)
}

infix fun InformationDatePresenter.inList(value: List<Date>): QueryListInformationLeaf<Date> {
    return QueryListInformationLeaf(ListCondition.inList, value, InnerType.Date, this)
}

infix fun InformationBooleanPresenter.inList(value: List<Boolean>): QueryListInformationLeaf<Boolean> {
    return QueryListInformationLeaf(ListCondition.inList, value, InnerType.Boolean, this)
}

infix fun InformationStringPresenter.inList(value: List<String>): QueryListInformationLeaf<String> {
    return QueryListInformationLeaf(ListCondition.inList, value, InnerType.String, this)
}

