package utils

import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import java.util.*

internal object InformationUtils {
    fun getPrimitiveInformation(id: Long, timeStamp: Long, version: Long, date: Date, isDeleted: Boolean = false): MutableSet<InformationObject<*>> {
        val result = HashSet<InformationObject<*>>()
        result.add(InformationObject(id, InformationType.TypesResolution.resolveType(IdPresenter)))
        result.add(InformationObject(timeStamp, InformationType.TypesResolution.resolveType(TimeStampPresenter)))
        result.add(InformationObject(version, InformationType.TypesResolution.resolveType(VersionPresenter)))
        result.add(InformationObject(date, InformationType.TypesResolution.resolveType(DatePresenter)))
        result.add(InformationObject(isDeleted, InformationType.TypesResolution.resolveType(IsDeletedPresenter)))
        return result
    }
}

