package tanvd.audit.implementation.clickhouse.model

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
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
    return SimpleDateFormat("yyyy-MM-dd")
}

internal fun getDateTimeFormat() : DateTimeFormatter {
    return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
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
            InnerType.DateTime -> {
                (it as DateTime).toStringSQL()
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