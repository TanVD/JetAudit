package utils

import tanvd.audit.model.external.types.information.InformationBooleanPresenter
import tanvd.audit.model.external.types.information.InformationPresenter
import tanvd.audit.model.external.types.information.InformationStringPresenter

internal object StringInfPresenter : InformationStringPresenter() {
    override val name: String = "StringInfPresenter"

    override fun getDefault(): String {
        return ""
    }
}

internal object BooleanInfPresenter : InformationBooleanPresenter() {
    override val name: String = "BooleanInfPresenter"

    override fun getDefault(): Boolean {
        return false
    }
}

