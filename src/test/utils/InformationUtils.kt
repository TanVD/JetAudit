package utils

import tanvd.audit.model.external.presenters.DateType
import tanvd.audit.model.external.presenters.IdType
import tanvd.audit.model.external.presenters.TimeStampType
import tanvd.audit.model.external.presenters.VersionType
import tanvd.audit.model.external.records.InformationObject
import java.util.*
import kotlin.collections.LinkedHashSet

internal object InformationUtils {
    fun getPrimitiveInformation(id: Long, timeStamp: Long, version: Long, date: Date): LinkedHashSet<InformationObject<*>> {
        val result = LinkedHashSet<InformationObject<*>>()
        result.add(InformationObject(id, IdType))
        result.add(InformationObject(timeStamp, TimeStampType))
        result.add(InformationObject(version, VersionType))
        result.add(InformationObject(date, DateType))
        return result
    }
}

