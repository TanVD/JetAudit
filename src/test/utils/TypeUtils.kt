package utils

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.presenters.IntPresenter
import tanvd.audit.model.external.presenters.LongPresenter
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.utils.PropertyLoader

object TypeUtils {
    fun clearTypes() {
        ObjectType.clearTypes()
        InformationType.clearTypes()
    }

    @Suppress("UNCHECKED_CAST")
    internal fun addAuditTypesPrimitive() {
        ObjectType.addType(ObjectType(String::class, StringPresenter) as ObjectType<Any>)
        ObjectType.addType(ObjectType(Int::class, IntPresenter) as ObjectType<Any>)
        ObjectType.addType(ObjectType(Long::class, LongPresenter) as ObjectType<Any>)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun addAuditTypePrimitive(auditDao: AuditDao) {
        auditDao.addTypeInDbModel(ObjectType(String::class, StringPresenter) as ObjectType<Any>)
        auditDao.addTypeInDbModel(ObjectType(Int::class, IntPresenter) as ObjectType<Any>)
        auditDao.addTypeInDbModel(ObjectType(Long::class, LongPresenter) as ObjectType<Any>)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun addInformationTypesPrimitive() {
        InformationType.addType(InformationType(IdPresenter,
                PropertyLoader.loadProperty("IdColumn") ?: "Id", InnerType.Long) as InformationType<Any>)
        InformationType.addType(InformationType(VersionPresenter,
                PropertyLoader.loadProperty("VersionColumn") ?: "Version", InnerType.ULong) as InformationType<Any>)
        InformationType.addType(InformationType(TimeStampPresenter,
                PropertyLoader.loadProperty("TimeStampColumn") ?: "TimeStamp",
                InnerType.Long) as InformationType<Any>)
    }
}
