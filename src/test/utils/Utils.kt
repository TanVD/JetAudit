package utils

import java.text.SimpleDateFormat
import java.util.*


fun getDate(date: String): Date {
    val dateFormat = SimpleDateFormat("dd/MM/yyyy")
    dateFormat.timeZone = TimeZone.getTimeZone("UTC")
    return dateFormat.parse(date)
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