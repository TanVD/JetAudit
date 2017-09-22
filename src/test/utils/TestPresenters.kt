package utils

import org.joda.time.DateTime
import tanvd.audit.model.external.types.information.*
import java.util.*

internal object LongInfPresenter : InformationLongPresenter() {
    override val code: String = "LongInfColumn"

    override fun getDefault(): Long {
        return 0
    }
}

internal object StringInfPresenter : InformationStringPresenter() {
    override val code: String = "StringInfColumn"

    override fun getDefault(): String {
        return ""
    }
}

internal object BooleanInfPresenter : InformationBooleanPresenter() {
    override val code: String = "BooleanInfColumn"

    override fun getDefault(): Boolean {
        return false
    }
}

internal object DateInfPresenter : InformationDatePresenter() {
    override val code: String = "DateInfColumn"

    override fun getDefault(): Date {
        return getDate("2000-01-01")
    }
}

internal object DateTimeInfPresenter : InformationDateTimePresenter() {
    override val code: String = "DateTimeInfColumn"

    override fun getDefault(): DateTime {
        return getDateTime("2000-01-01 12:00:00")
    }
}

