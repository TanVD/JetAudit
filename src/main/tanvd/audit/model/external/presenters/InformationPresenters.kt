package tanvd.audit.model.external.presenters

import tanvd.audit.model.external.types.information.InformationDatePresenter
import tanvd.audit.model.external.types.information.InformationLongPresenter
import tanvd.audit.utils.PropertyLoader
import tanvd.audit.utils.RandomGenerator
import java.util.*

object TimeStampPresenter : InformationLongPresenter() {

    override val code by lazy { PropertyLoader.loadProperty("TimeStampColumn") ?: "TimeStampColumn" }

    override fun getDefault(): Long {
        return System.currentTimeMillis()
    }
}

object VersionPresenter : InformationLongPresenter() {

    override val code by lazy { PropertyLoader.loadProperty("VersionColumn") ?: "VersionColumn" }

    override fun getDefault(): Long {
        return 0
    }
}

object IdPresenter : InformationLongPresenter() {

    override val code by lazy { PropertyLoader.loadProperty("IdColumn") ?: "IdColumn" }

    override fun getDefault(): Long {
        return RandomGenerator.next()
    }
}

object DatePresenter : InformationDatePresenter() {

    override val code by lazy { PropertyLoader.loadProperty("DateColumn") ?: "DateColumn" }

    override fun getDefault(): Date {
        return Date()
    }
}
