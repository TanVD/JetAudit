package utils

import java.text.SimpleDateFormat
import java.util.*


fun getDate(date: String): Date {
    val dateFormat = SimpleDateFormat("dd/MM/yyyy")
    dateFormat.timeZone = TimeZone.getTimeZone("UTC")
    return dateFormat.parse(date)
}