package tanvd.audit.model.external.queries

import tanvd.aorm.query.*
import tanvd.audit.model.external.types.information.*


//Equality interface
infix fun <T : Any>InformationType<T>.equal(value: T): QueryExpression {
    return column eq value
}

//String interface:
infix fun InformationType<String>.like(value: String): QueryExpression {
    return column like value
}

infix fun InformationType<String>.regex(value: String): QueryExpression {
    return column regex value
}

//Less or eq interface
infix fun <T : Any>InformationType<T>.less(value: T): QueryExpression {
    return column less value
}

infix fun <T: Any>InformationType<T>.lessOrEq(value: T): QueryExpression {
    return column lessOrEq value
}

infix fun <T: Any>InformationType<T>.more(value: T): QueryExpression {
    return column more value
}

infix fun <T: Any>InformationType<T>.moreOrEq(value: T): QueryExpression {
    return column moreOrEq value
}

//List interface
infix fun <T: Any>InformationType<T>.inList(values: List<T>) : QueryExpression {
    return column inList values
}
