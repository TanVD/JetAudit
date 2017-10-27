package tanvd.audit.implementation.clickhouse.model

import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.types.InnerType

fun QueryTypeLeafCondition<*>.toStringSQL(): String {
    return when (this) {
        is QueryEqualityTypeLeaf<*> -> {
            toStringSQL()
        }
        is QueryStringTypeLeaf<*> -> {
            toStringSQL()
        }
        is QueryNumberTypeLeaf<*> -> {
            toStringSQL()
        }
        is QueryListTypeLeaf<*> -> {
            toStringSQL()
        }
    }
}

fun QueryEqualityTypeLeaf<*>.toStringSQL(): String {
    return when (condition as EqualityCondition) {
        EqualityCondition.equal -> {
            when (valueType) {
                InnerType.Long -> {
                    "has(${stateType.getCode()}, ?)"
                }
                InnerType.String -> {
                    "has(${stateType.getCode()}, ?)"
                }
                InnerType.Boolean -> {
                    "has(${stateType.getCode()}, ?)"
                }
                else -> {
                    throw UnsupportedOperationException("Equality queries for type $valueType not supported")
                }
            }
        }
    }
}

fun QueryStringTypeLeaf<*>.toStringSQL(): String {
    return when (condition as StringCondition) {
        StringCondition.like -> {
            when (valueType) {
                InnerType.String -> {
                    "arrayExists((x) -> like(x, ?), ${stateType.getCode()})"
                }
                else -> {
                    throw UnsupportedOperationException("String queries for type $valueType not supported")
                }
            }
        }
        StringCondition.regexp -> {
            when (valueType) {
                InnerType.String -> {
                    "arrayExists((x) -> match(x, ?), ${stateType.getCode()})"
                }
                else -> {
                    throw UnsupportedOperationException("String queries for type $valueType not supported")
                }
            }
        }
    }
}

fun QueryNumberTypeLeaf<*>.toStringSQL(): String {
    return when (condition as NumberCondition) {
        NumberCondition.less -> {
            when (valueType) {
                InnerType.Long -> {
                    "arrayExists((x) -> x < ?, ${stateType.getCode()})"
                }
                else -> {
                    throw UnsupportedOperationException("Number queries for type $valueType not supported")
                }
            }
        }
        NumberCondition.more -> {
            when (valueType) {
                InnerType.Long -> {
                    "arrayExists((x) -> x > ?, ${stateType.getCode()})"
                }
                else -> {
                    throw UnsupportedOperationException("Number queries for type $valueType not supported")
                }
            }
        }
    }
}

@Suppress("UNCHECKED_CAST")
fun QueryListTypeLeaf<*>.toStringSQL(): String {
    return when (condition as ListCondition) {
        ListCondition.inList -> {
            when (valueType) {
                InnerType.Long -> {
                    "arrayExists((x) -> x in (${(value as List<*>).joinToString { "?" }}), ${stateType.getCode()})"
                }
                InnerType.String -> {
                    "arrayExists((x) -> x in (${(value as List<*>).joinToString { "?" }}), ${stateType.getCode()})"
                }
                InnerType.Boolean -> {
                    "arrayExists((x) -> x in (${(value as List<*>).joinToString { "?" }}), ${stateType.getCode()})"
                }
                else -> {
                    throw UnsupportedOperationException("List queries for type $valueType not supported")
                }
            }
        }
    }
}