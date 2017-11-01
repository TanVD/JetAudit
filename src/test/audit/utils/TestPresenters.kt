package audit.utils

import org.joda.time.DateTime
import tanvd.aorm.*
import tanvd.audit.model.external.types.information.*
import utils.getDate
import utils.getDateTime
import java.util.*

internal object LongInf : InformationType<Long>("LongInfColumn", DbLong(), { 0 })

internal object StringInf : InformationType<String>("StringInfColumn", DbString(), { "" })

internal object BooleanInf : InformationType<Boolean>("BooleanInfColumn", DbBoolean(), { false })

internal object DateInf : InformationType<Date>("DateInfColumn", DbDate(), { getDate("2000-01-01") })

internal object DateTimeInf : InformationType<DateTime>("DateTimeInfColumn", DbDateTime(), { getDateTime("2000-01-01 12:00:00") })

