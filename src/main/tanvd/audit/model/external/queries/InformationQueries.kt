package tanvd.audit.model.external.queries

import tanvd.audit.model.external.types.information.*

//InformationLongLeaf

class QueryInformationLongLeaf(val condition: QueryLongCondition, val value: Long,
                               presenter: InformationPresenter<*>) : QueryExpression {
    val type = InformationType.resolveType(presenter)
}


infix fun InformationLongPresenter.less(value: Long): QueryInformationLongLeaf {
    return QueryInformationLongLeaf(QueryLongCondition.less, value, this)
}

infix fun InformationLongPresenter.more(value: Long): QueryInformationLongLeaf {
    return QueryInformationLongLeaf(QueryLongCondition.more, value, this)
}

infix fun InformationLongPresenter.equal(value: Long): QueryInformationLongLeaf {
    return QueryInformationLongLeaf(QueryLongCondition.equal, value, this)
}

//InformationStringLeaf

class QueryInformationStringLeaf(val condition: QueryStringCondition, val value: String,
                                 presenter: InformationPresenter<*>) : QueryExpression {
    val type = InformationType.resolveType(presenter)
}


infix fun InformationStringPresenter.like(value: String): QueryInformationStringLeaf {
    return QueryInformationStringLeaf(QueryStringCondition.like, value, this)
}

infix fun InformationStringPresenter.equal(value: String): QueryInformationStringLeaf {
    return QueryInformationStringLeaf(QueryStringCondition.equal, value, this)
}

infix fun InformationStringPresenter.regexp(value: String): QueryInformationStringLeaf {
    return QueryInformationStringLeaf(QueryStringCondition.regexp, value, this)
}

//InformationBooleanLeaf

class QueryInformationBooleanLeaf(val condition: QueryBooleanCondition, val value: Boolean,
                                  presenter: InformationPresenter<*>) : QueryExpression {
    val type = InformationType.resolveType(presenter)
}


infix fun InformationBooleanPresenter.`is`(value: Boolean): QueryInformationBooleanLeaf {
    return QueryInformationBooleanLeaf(QueryBooleanCondition.`is`, value, this)
}

infix fun InformationBooleanPresenter.isNot(value: Boolean): QueryInformationBooleanLeaf {
    return QueryInformationBooleanLeaf(QueryBooleanCondition.isNot, value, this)
}
