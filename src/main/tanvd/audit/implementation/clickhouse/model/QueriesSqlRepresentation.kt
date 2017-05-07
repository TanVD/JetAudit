package tanvd.audit.implementation.clickhouse.model

import tanvd.audit.model.external.queries.*


fun QueryInformationLongLeaf.toStringSQL(): String {
    return when (condition) {
        QueryLongCondition.less -> {
            "${this.type.code} < $value"
        }
        QueryLongCondition.more -> {
            "${this.type.code} > $value"
        }
        QueryLongCondition.equal -> {
            "${this.type.code} = $value"
        }
    }
}

fun QueryInformationBooleanLeaf.toStringSQL(): String {
    return when (condition) {
        QueryBooleanCondition.`is` -> {
            "${this.type.code} == ${if (this.value) "1" else "0"}"
        }
        QueryBooleanCondition.isNot -> {
            "${this.type.code} != ${if (this.value) "1" else "0"}"
        }
    }
}

fun QueryInformationStringLeaf.toStringSQL(): String {
    return when (condition) {
        QueryStringCondition.equal -> {
            "${type.code} == ?"
        }
        QueryStringCondition.like -> {
            "like(${type.code}, ?)"
        }
        QueryStringCondition.regexp -> {
            "match(${type.code}, ?)"
        }
    }
}

fun QueryTypeLongLeaf.toStringSQL(): String {
    return when (typeCondition) {
        QueryLongCondition.less -> {
            "arrayExists((x) -> x < $id, ${stateType.getCode()})"
        }
        QueryLongCondition.more -> {
            "arrayExists((x) -> x > $id, ${stateType.getCode()})"
        }
        QueryLongCondition.equal -> {
            "has(${stateType.getCode()}, $id)"
        }
    }
}


fun QueryTypeBooleanLeaf.toStringSQL(): String {
    return when (typeCondition) {
        QueryBooleanCondition.`is` -> {
            "has(${stateType.getCode()}, ${if (id.toBoolean()) "1" else "0"})"
        }
        QueryBooleanCondition.isNot -> {
            "has(${stateType.getCode()}, ${if (!id.toBoolean()) "1" else "0"})"
        }
    }
}


fun QueryTypeStringLeaf.toStringSQL(): String {
    return when (typeCondition) {
        QueryStringCondition.equal -> {
            "has(${stateType.getCode()}, ?)"
        }
        QueryStringCondition.like -> {
            "arrayExists((x) -> like(x, ?), ${stateType.getCode()})"
        }
        QueryStringCondition.regexp -> {
            "arrayExists((x) -> match(x, ?), ${stateType.getCode()})"
        }
    }
}

