package utils

import tanvd.audit.model.external.presenters.DatePresenter
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import java.util.*

internal object InformationUtils {
    fun getPrimitiveInformation(id: Long, timeStamp: Long, version: Long, date: Date): MutableSet<InformationObject<*>> {
        val result = HashSet<InformationObject<*>>()
        result.add(InformationObject(id, InformationType.TypesResolution.resolveType(IdPresenter)))
        result.add(InformationObject(timeStamp, InformationType.TypesResolution.resolveType(TimeStampPresenter)))
        result.add(InformationObject(version, InformationType.TypesResolution.resolveType(VersionPresenter)))
        result.add(InformationObject(date, InformationType.TypesResolution.resolveType(DatePresenter)))
        return result
    }
}

