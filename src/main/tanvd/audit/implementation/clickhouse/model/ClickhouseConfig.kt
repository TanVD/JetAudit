package tanvd.audit.implementation.clickhouse.model

import tanvd.audit.utils.PropertyLoader
import java.util.*


object ClickhouseConfig {
    val timeZone: TimeZone
        get() {
            return TimeZone.getTimeZone(PropertyLoader["ServerTimeZone"] ?: "Zulu")
        }
}