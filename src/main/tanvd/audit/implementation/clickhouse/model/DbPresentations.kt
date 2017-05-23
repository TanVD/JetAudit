package tanvd.audit.implementation.clickhouse.model

import ru.yandex.clickhouse.ClickHouseUtil
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.objects.StateType
import java.text.DateFormat
import java.text.SimpleDateFormat


internal fun StateType<*>.getCode(): String {
    return this.objectName + "_" + this.stateName
}

internal fun getDateFormat(): DateFormat {
    return SimpleDateFormat("yyyy-MM-dd")
}

internal fun Boolean.toSqlString(): String {
    return if (this) {
        "1"
    } else {
        "0"
    }
}

internal fun booleanFromSqlString(sql: String): Boolean {
    return sql == "1"
}

internal fun String.toSanitizedStringSQL(): String {
    return "\'" + ClickHouseUtil.escape(this) + "\'"
}

internal fun List<Any>.toSanitizedSetSQL(type: InnerType): String {
    return map {
        when (type) {
            InnerType.String -> {
                "\'" + ClickHouseUtil.escape(it.toString()) + "\'"
            }
            InnerType.Boolean -> {
                (it as Boolean).toSqlString()
            }
            else -> {
                it.toString()
            }
        }
    }.joinToString(prefix = "(", postfix = ")")
}