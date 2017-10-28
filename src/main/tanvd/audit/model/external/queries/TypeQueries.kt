package tanvd.audit.model.external.queries

import tanvd.aorm.query.*
import tanvd.audit.model.external.types.objects.StateType

//Implementations
infix fun <T: Any>StateType<T>.equal(number: T): QueryExpression {
    return column exists { x -> x eq number }
}

//String interface
infix fun StateType<String>.like(value: String): QueryExpression {
    return column exists { x -> x like value}
}

infix fun StateType<String>.regex(pattern: String): QueryExpression {
    return column exists { x -> x regex pattern }
}

//Number interface
infix fun <T: Number>StateType<T>.less(value: T): QueryExpression {
    return column exists { x -> x less value}
}

infix fun <T: Number>StateType<T>.lessOrEq(value: T): QueryExpression {
    return column exists { x -> x lessOrEq  value}
}

infix fun <T: Number>StateType<T>.more(value: T): QueryExpression {
    return column exists { x -> x more value}
}

infix fun <T: Number>StateType<T>.moreOrEq(value: T): QueryExpression {
    return column exists { x -> x moreOrEq  value}
}

//List interface
infix fun <T: Any> StateType<T>.inList(value: List<T>): QueryExpression {
    return column exists {x -> x inList value}
}
