package tanvd.audit.model.external.queries

interface InnerTypeCondition

//NumberCondition

enum class NumberCondition : InnerTypeCondition {
    less,
    more;
}

//StringCondition

enum class StringCondition : InnerTypeCondition {
    like,
    regexp;
}

//EqualityCondition
enum class EqualityCondition : InnerTypeCondition {
    equal,
    notEqual;
}

//ListCondition
enum class ListCondition : InnerTypeCondition {
    inList,
    notInList;
}
