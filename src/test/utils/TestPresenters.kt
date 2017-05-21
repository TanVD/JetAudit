package utils

import tanvd.audit.model.external.types.information.InformationBooleanPresenter
import tanvd.audit.model.external.types.information.InformationStringPresenter

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

