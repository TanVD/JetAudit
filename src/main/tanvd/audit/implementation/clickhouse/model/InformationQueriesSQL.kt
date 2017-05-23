package tanvd.audit.implementation.clickhouse.model

import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.types.InnerType

fun QueryInformationLeafCondition<*>.toStringSQL(): String {
    return when (this) {
        is QueryEqualityInformationLeaf -> {
            toStringSQL()
        }
        is QueryStringInformationLeaf -> {
            toStringSQL()
        }
        is QueryNumberInformationLeaf -> {
            toStringSQL()
        }
        is QueryListInformationLeaf -> {
            toStringSQL()
        }
    }
}

fun QueryEqualityInformationLeaf<*>.toStringSQL(): String {
    return when (condition as EqualityCondition) {
        EqualityCondition.equal -> {
            when (valueType) {
                InnerType.Long -> {
                    "${presenter.code} == $value"
                }
                InnerType.String -> {
                    "${presenter.code} == ${(value as String).toSanitizedStringSQL()}"
                }
                InnerType.Boolean -> {
                    "${presenter.code} == ${(value as Boolean).toSqlString()}"
                }
                else -> {
                    throw UnsupportedOperationException("Equality queries for information $valueType not supported")
                }
            }
        }
        EqualityCondition.notEqual -> {
            when (valueType) {
                InnerType.Long -> {
                    "${presenter.code} != $value"
                }
                InnerType.String -> {
                    "${presenter.code} != ${(value as String).toSanitizedStringSQL()}"
                }
                InnerType.Boolean -> {
                    "${presenter.code} != ${(value as Boolean).toSqlString()}"
                }
                else -> {
                    throw UnsupportedOperationException("Equality queries for information $valueType not supported")
                }
            }
        }
    }
}

fun QueryStringInformationLeaf<*>.toStringSQL(): String {
    return when (condition as StringCondition) {
        StringCondition.like -> {
            when (valueType) {
                InnerType.String -> {
                    "like(${presenter.code}, ${(value as String).toSanitizedStringSQL()})"
                }
                else -> {
                    throw UnsupportedOperationException("String queries for type $valueType not supported")
                }
            }
        }
        StringCondition.regexp -> {
            when (valueType) {
                InnerType.String -> {
                    "match(${presenter.code}, ${(value as String).toSanitizedStringSQL()})"
                }
                else -> {
                    throw UnsupportedOperationException("String queries for type $valueType not supported")
                }
            }
        }
    }
}

fun QueryNumberInformationLeaf<*>.toStringSQL(): String {
    return when (condition as NumberCondition) {
        NumberCondition.less -> {
            when (valueType) {
                InnerType.Long -> {
                    "${presenter.code} < $value"
                }
                else -> {
                    throw UnsupportedOperationException("Number queries for type $valueType not supported")
                }
            }
        }
        NumberCondition.more -> {
            when (valueType) {
                InnerType.Long -> {
                    "${presenter.code} > $value"
                }
                else -> {
                    throw UnsupportedOperationException("Number queries for type $valueType not supported")
                }
            }
        }
    }
}

@Suppress("UNCHECKED_CAST")
fun QueryListInformationLeaf<*>.toStringSQL(): String {
    return when (condition as ListCondition) {
        ListCondition.inList -> {
            when (valueType) {
                InnerType.Long -> {
                    "${presenter.code} in ${(value as List<Any>).toSanitizedSetSQL(valueType)}"
                }
                InnerType.String -> {
                    "${presenter.code} in ${(value as List<Any>).toSanitizedSetSQL(valueType)}"
                }
                InnerType.Boolean -> {
                    "${presenter.code} in ${(value as List<Any>).toSanitizedSetSQL(valueType)}"
                }
                else -> {
                    throw UnsupportedOperationException("List queries for type $valueType not supported")
                }
            }
        }
        ListCondition.notInList -> {
            when (valueType) {
                InnerType.Long -> {
                    "not (${presenter.code} in ${(value as List<Any>).toSanitizedSetSQL(valueType)})"
                }
                InnerType.String -> {
                    "not (${presenter.code} in ${(value as List<Any>).toSanitizedSetSQL(valueType)})"
                }
                InnerType.Boolean -> {
                    "not (${presenter.code} in ${(value as List<Any>).toSanitizedSetSQL(valueType)})"
                }
                else -> {
                    throw UnsupportedOperationException("List queries for type $valueType not supported")
                }
            }
        }
    }
}