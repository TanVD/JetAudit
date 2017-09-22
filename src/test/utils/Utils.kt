package utils

import org.joda.time.DateTime
import tanvd.audit.implementation.clickhouse.model.getDateFormat
import tanvd.audit.implementation.clickhouse.model.getDateTimeFormat
import java.util.*


fun getDate(date: String): Date {
    return getDateFormat().parse(date)
}

fun getDateTime(dateTime: String): DateTime {
    return getDateTimeFormat().parseDateTime(dateTime)
}

fun measureTime(func: () -> Unit) : Long {
    val time = System.currentTimeMillis()
    func()
    return System.currentTimeMillis() - time
}

fun waitUntilRightCount(func: () -> Boolean, sleepTime: Long, totalNumber: Long) {
    var current = 0L
    while (!func()) {
        if (current == totalNumber) {
            return
        }
        Thread.sleep(sleepTime)
        current++
    }
}