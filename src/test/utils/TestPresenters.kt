package utils

import tanvd.audit.model.external.types.InformationPresenter

internal object StringPresenter : InformationPresenter<String>() {
    override val name: String = "StringPresenter"

    override fun getDefault(): String {
        return ""
    }
}

internal object BooleanPresenter : InformationPresenter<Boolean>() {
    override val name: String = "BooleanPresenter"

    override fun getDefault(): Boolean {
        return false
    }
}

