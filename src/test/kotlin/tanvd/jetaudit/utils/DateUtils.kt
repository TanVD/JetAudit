package tanvd.jetaudit.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.sql.Date
import java.text.SimpleDateFormat

/** Pattern yyyy-MM-dd. **/
fun getDate(date: String): Date = Date(SimpleDateFormat("yyyy-MM-dd").parse(date).time)

/** Pattern yyyy-MM-dd HH:mm:ss. **/
fun getDateTime(dateTime: String): DateTime = DateTime.parse(dateTime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))