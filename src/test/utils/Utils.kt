package utils

import java.text.SimpleDateFormat
import java.util.*


fun getDate(date: String): Date {
    return SimpleDateFormat("dd/MM/yyyy").parse(date)
}