package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.types.information.InformationLongPresenter
import tanvd.audit.model.external.types.information.InformationPresenter
import tanvd.audit.utils.RandomGenerator

object TimeStampPresenter : InformationLongPresenter() {
    override fun getDefault(): Long {
        return System.currentTimeMillis()
    }

    override val name = "TimeStampPresenter"
}

object VersionPresenter : InformationLongPresenter() {
    override fun getDefault(): Long {
        return 0
    }

    override val name = "VersionPresenter"
}

object IdPresenter : InformationLongPresenter() {
    override fun getDefault(): Long {
        return RandomGenerator.next()
    }

    override val name = "IdPresenter"
}
