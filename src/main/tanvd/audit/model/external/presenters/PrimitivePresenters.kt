package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.types.InformationPresenter
import tanvd.audit.utils.RandomGenerator

object TimeStampPresenter : InformationPresenter<Long>() {
    override fun getDefault(): Long {
        return System.currentTimeMillis()
    }

    override val name = "TimeStampPresenter"
}

object VersionPresenter : InformationPresenter<Long>() {
    override fun getDefault(): Long {
        return 0
    }

    override val name = "VersionPresenter"
}

object IdPresenter : InformationPresenter<Long>() {
    override fun getDefault(): Long {
        return RandomGenerator.next()
    }

    override val name = "IdPresenter"
}
