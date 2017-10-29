package utils

import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import java.util.*

internal object InformationUtils {
    fun getPrimitiveInformation(id: Long, timeStamp: Long, version: Long, date: Date, isDeleted: Boolean = false): MutableSet<InformationObject<*>> {
        val result = HashSet<InformationObject<*>>()
        result.add(InformationObject(id, IdType))
        result.add(InformationObject(timeStamp, TimeStampType))
        result.add(InformationObject(version, VersionType))
        result.add(InformationObject(date, DateType))
        result.add(InformationObject(isDeleted, IsDeletedType))
        return result
    }
}

