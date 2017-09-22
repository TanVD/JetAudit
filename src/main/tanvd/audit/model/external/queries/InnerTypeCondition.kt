package tanvd.audit.model.external.queries

interface InnerTypeCondition

//NumberCondition

enum class NumberCondition : InnerTypeCondition {
    less,
    more;
}

//DateCondition
enum class TimeCondition : InnerTypeCondition {
    less,
    lessOrEqual,
    more,
    moreOrEqual;
}

//StringCondition

enum class StringCondition : InnerTypeCondition {
    like,
    regexp;
}

//EqualityCondition
enum class EqualityCondition : InnerTypeCondition {
    equal
}

//ListCondition
enum class ListCondition : InnerTypeCondition {
    inList
}
