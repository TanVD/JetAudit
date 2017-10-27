package tanvd.audit.implementation.clickhouse.model

import org.joda.time.DateTime
import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.types.InnerType
import java.util.*

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
        is QueryTimeInformationLeaf -> {
            toStringSQL()
        }
    }
}

fun QueryEqualityInformationLeaf<*>.toStringSQL(): String {
    return when (condition as EqualityCondition) {
        EqualityCondition.equal -> {
            when (valueType) {
                InnerType.Long -> {
                    "${presenter.code} == ?"
                }
                InnerType.String -> {
                    "${presenter.code} == ?"
                }
                InnerType.Boolean -> {
                    "${presenter.code} == ?"
                }
                InnerType.Date -> {
                    "${presenter.code} == ?"
                }
                InnerType.DateTime -> {
                    "${presenter.code} == ?"
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
                    "like(${presenter.code}, ?)"
                }
                else -> {
                    throw UnsupportedOperationException("String queries for type $valueType not supported")
                }
            }
        }
        StringCondition.regexp -> {
            when (valueType) {
                InnerType.String -> {
                    "match(${presenter.code}, ?)"
                }
                else -> {
                    throw UnsupportedOperationException("String queries for type $valueType not supported")
                }
            }
        }
    }
}

fun QueryTimeInformationLeaf<*>.toStringSQL(): String {
    return when (condition as TimeCondition) {
        TimeCondition.less -> {
            when (valueType) {
                InnerType.Date -> {
                    "${presenter.code} < ?"
                }
                InnerType.DateTime -> {
                    "${presenter.code} < ?"
                }
                else -> {
                    throw UnsupportedOperationException("Date queries for type $valueType not supported")
                }
            }
        }
        TimeCondition.more -> {
            when (valueType) {
                InnerType.Date -> {
                    "${presenter.code} > ?"
                }
                InnerType.DateTime -> {
                    "${presenter.code} > ?"
                }
                else -> {
                    throw UnsupportedOperationException("Time queries for type $valueType not supported")
                }
            }
        }
        TimeCondition.lessOrEqual -> {
            when (valueType) {
                InnerType.Date -> {
                    "${presenter.code} <= ?"
                }
                InnerType.DateTime -> {
                    "${presenter.code} <= ?"
                }
                else -> {
                    throw UnsupportedOperationException("Time queries for type $valueType not supported")
                }
            }
        }
        TimeCondition.moreOrEqual -> {
            when (valueType) {
                InnerType.Date -> {
                    "${presenter.code} >= ?"
                }
                InnerType.DateTime -> {
                    "${presenter.code} >= ?"
                }
                else -> {
                    throw UnsupportedOperationException("Time queries for type $valueType not supported")
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
                    "${presenter.code} < ?"
                }
                else -> {
                    throw UnsupportedOperationException("Number queries for type $valueType not supported")
                }
            }
        }
        NumberCondition.more -> {
            when (valueType) {
                InnerType.Long -> {
                    "${presenter.code} > ?"
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
                    "${presenter.code} in (${(value as List<*>).joinToString { "?" }})"
                }
                InnerType.String -> {
                    "${presenter.code} in (${(value as List<*>).joinToString { "?" }})"
                }
                InnerType.Boolean -> {
                    "${presenter.code} in (${(value as List<*>).joinToString { "?" }})"
                }
                InnerType.Date -> {
                    "${presenter.code} in (${(value as List<*>).joinToString { "?" }})"
                }
                InnerType.DateTime -> {
                    "${presenter.code} in (${(value as List<*>).joinToString { "?" }})"
                }
                else -> {
                    throw UnsupportedOperationException("List queries for type $valueType not supported")
                }
            }
        }
    }
}