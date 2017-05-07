package tanvd.audit.implementation.clickhouse.model

import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.StateType

internal fun InformationType<*>.toDbColumnType(): DbColumnType {
    return when (type) {
        InnerType.Long -> {
            DbColumnType.DbLong
        }
        InnerType.String -> {
            DbColumnType.DbString
        }
        InnerType.Boolean -> {
            DbColumnType.DbBoolean
        }
        InnerType.ULong -> {
            DbColumnType.DbULong
        }
    }
}

internal fun StateType<*>.toDbColumnType(): DbColumnType {
    return when (type) {
        InnerType.Long -> {
            DbColumnType.DbArrayLong
        }
        InnerType.String -> {
            DbColumnType.DbArrayString
        }
        InnerType.Boolean -> {
            DbColumnType.DbArrayBoolean
        }
        InnerType.ULong -> {
            DbColumnType.DbArrayULong
        }
    }
}

internal fun InformationType<*>.toValue(serialized: String): Any {
    return when (type) {
        InnerType.Long -> {
            serialized.toLong()
        }
        InnerType.String -> {
            serialized
        }
        InnerType.Boolean -> {
            serialized.toBoolean()
        }
        InnerType.ULong -> {
            serialized.toLong()
        }
    }
}