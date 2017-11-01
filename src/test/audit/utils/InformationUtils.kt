package audit.utils

import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import java.util.*
import kotlin.collections.LinkedHashSet

internal object InformationUtils {
    fun getPrimitiveInformation(id: Long, timeStamp: Long, version: Long, date: Date, isDeleted: Boolean = false): LinkedHashSet<InformationObject<*>> {
        val result = LinkedHashSet<InformationObject<*>>()
        result.add(InformationObject(id, IdType))
        result.add(InformationObject(timeStamp, TimeStampType))
        result.add(InformationObject(version, VersionType))
        result.add(InformationObject(date, DateType))
        result.add(InformationObject(isDeleted, IsDeletedType))
        return result
    }
}

