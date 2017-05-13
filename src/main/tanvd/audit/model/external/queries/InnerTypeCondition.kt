package tanvd.audit.model.external.queries

//QueryLongCondition

enum class QueryLongCondition {
    less,
    more,
    equal;
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

//QueryBooleanCondition

enum class QueryBooleanCondition {
    `is`,
    isNot
}
