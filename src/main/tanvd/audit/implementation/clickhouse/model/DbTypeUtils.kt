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
        InnerType.Date -> {
            DbColumnType.DbDate
        }
        InnerType.DateTime -> {
            DbColumnType.DbDateTime
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
        InnerType.Date -> {
            DbColumnType.DbArrayDate
        }
        InnerType.DateTime -> {
            DbColumnType.DbArrayDateTime
        }
    }
}