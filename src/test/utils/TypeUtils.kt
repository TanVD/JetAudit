package utils

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType

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
        InformationType.addType(InformationType(IdType, InnerType.Long) as InformationType<Any>)
        InformationType.addType(InformationType(VersionType, InnerType.ULong) as InformationType<Any>)
        InformationType.addType(InformationType(TimeStampType, InnerType.Long) as InformationType<Any>)
        InformationType.addType(InformationType(DateType, InnerType.Date) as InformationType<Any>)
        InformationType.addType(InformationType(IsDeletedType, InnerType.Boolean) as InformationType<Any>)
    }
}
