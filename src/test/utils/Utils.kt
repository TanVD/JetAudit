package utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.text.SimpleDateFormat
import java.util.*


fun getDate(date: String): Date {
    return SimpleDateFormat("yyyy-MM-dd").parse(date)
}

fun getDateTime(dateTime: String): DateTime {
    return DateTime.parse(dateTime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
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