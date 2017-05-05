package tanvd.audit.model.external.queries

import tanvd.audit.model.external.types.InformationPresenter
import tanvd.audit.model.external.types.InformationType


//QueryLongCondition

enum class QueryLongCondition {
    less,
    more,
    equal;
}

class QueryInformationLongLeaf(val condition: QueryLongCondition, val value: Long,
                               presenter: InformationPresenter<*>) : QueryExpression {
    val type = InformationType.resolveType(presenter)
}


infix fun InformationPresenter<Long>.less(value: Long): QueryInformationLongLeaf {
    return QueryInformationLongLeaf(QueryLongCondition.less, value, this)
}

infix fun InformationPresenter<Long>.more(value: Long): QueryInformationLongLeaf {
    return QueryInformationLongLeaf(QueryLongCondition.more, value, this)
}

infix fun InformationPresenter<Long>.equal(value: Long): QueryInformationLongLeaf {
    return QueryInformationLongLeaf(QueryLongCondition.equal, value, this)
}


//QueryStringCondition

enum class QueryStringCondition {
    like,
    equal,
    /**
     * Beware, it is not a full match. True if got partial match
     */
    regexp
}

class QueryInformationStringLeaf(val condition: QueryStringCondition, val value: String,
                                 presenter: InformationPresenter<*>) : QueryExpression {
    val type = InformationType.resolveType(presenter)
}


infix fun InformationPresenter<String>.like(value: String): QueryInformationStringLeaf {
    return QueryInformationStringLeaf(QueryStringCondition.like, value, this)
}

infix fun InformationPresenter<String>.equal(value: String): QueryInformationStringLeaf {
    return QueryInformationStringLeaf(QueryStringCondition.equal, value, this)
}

infix fun InformationPresenter<String>.regexp(value: String): QueryInformationStringLeaf {
    return QueryInformationStringLeaf(QueryStringCondition.regexp, value, this)
}

//QueryBooleanCondition

enum class QueryBooleanCondition {
    `is`,
    isNot
}

class QueryInformationBooleanLeaf(val condition: QueryBooleanCondition, val value: Boolean,
                                  presenter: InformationPresenter<*>) : QueryExpression {
    val type = InformationType.resolveType(presenter)
}


infix fun InformationPresenter<Boolean>.`is`(value: Boolean): QueryInformationBooleanLeaf {
    return QueryInformationBooleanLeaf(QueryBooleanCondition.`is`, value, this)
}

infix fun InformationPresenter<Boolean>.isNot(value: Boolean): QueryInformationBooleanLeaf {
    return QueryInformationBooleanLeaf(QueryBooleanCondition.isNot, value, this)
}
