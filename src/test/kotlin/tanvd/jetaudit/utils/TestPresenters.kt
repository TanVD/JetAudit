package tanvd.jetaudit.utils

import org.joda.time.DateTime
import tanvd.aorm.*
import tanvd.jetaudit.model.external.types.information.InformationType
import java.util.*

internal object LongInf : InformationType<Long>("LongInfColumn", DbInt64, { 0 })

internal object StringInf : InformationType<String>("StringInfColumn", DbString(), { "" })

internal object BooleanInf : InformationType<Boolean>("BooleanInfColumn", DbBoolean(), { false })

internal object DateInf : InformationType<Date>("DateInfColumn", DbDate(), { getDate("2000-01-01") })

internal object DateTimeInf : InformationType<DateTime>("DateTimeInfColumn", DbDateTime(), { getDateTime("2000-01-01 12:00:00") })

