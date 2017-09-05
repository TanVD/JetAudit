package tanvd.audit.implementation.clickhouse.model

import ru.yandex.clickhouse.ClickHouseUtil
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.objects.StateType
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*


internal fun StateType<*>.getCode(): String {
    return this.objectName + "_" + this.stateName
}

internal fun getDateFormat(): DateFormat {
    val dateFormat = SimpleDateFormat("yyyy-MM-dd")
    dateFormat.timeZone = TimeZone.getTimeZone("UTC")
    return dateFormat
}

internal fun Boolean.toStringSQL(): String {
    return if (this) {
        "1"
    } else {
        "0"
    }
}

internal fun String.toSanitizedStringSQL(): String {
    return "\'" + ClickHouseUtil.escape(this) + "\'"
}

internal fun List<Any>.toSanitizedSetSQL(type: InnerType): String {
    return map {
        when (type) {
            InnerType.String -> {
                (it as String).toSanitizedStringSQL()
            }
            InnerType.Date -> {
                (it as Date).toStringSQL()
            }
            InnerType.Boolean -> {
                (it as Boolean).toStringSQL()
            }
            else -> {
                it.toString()
            }
        }
    }.joinToString(prefix = "(", postfix = ")")
}