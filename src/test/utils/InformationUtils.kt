package utils

import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import java.util.*

internal object InformationUtils {
    fun getPrimitiveInformation(id: Long, timeStamp: Long, version: Long, date: Date, isDeleted: Boolean = false): MutableSet<InformationObject<*>> {
        val result = HashSet<InformationObject<*>>()
        result.add(InformationObject(id, InformationType.TypesResolution.resolveType(IdType)))
        result.add(InformationObject(timeStamp, InformationType.TypesResolution.resolveType(TimeStampType)))
        result.add(InformationObject(version, InformationType.TypesResolution.resolveType(VersionType)))
        result.add(InformationObject(date, InformationType.TypesResolution.resolveType(DateType)))
        result.add(InformationObject(isDeleted, InformationType.TypesResolution.resolveType(IsDeletedType)))
        return result
    }
}

