package utils

import tanvd.audit.model.external.types.information.InformationBooleanPresenter
import tanvd.audit.model.external.types.information.InformationDatePresenter
import tanvd.audit.model.external.types.information.InformationLongPresenter
import tanvd.audit.model.external.types.information.InformationStringPresenter
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
        return getDate("01/01/2000")
    }
}

