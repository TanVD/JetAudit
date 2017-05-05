package utils

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.serializers.IntSerializer
import tanvd.audit.model.external.serializers.LongSerializer
import tanvd.audit.model.external.serializers.StringSerializer
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.utils.PropertyLoader

object TypeUtils {
    fun clearTypes() {
        AuditType.clearTypes()
        InformationType.clearTypes()
    }

    @Suppress("UNCHECKED_CAST")
    internal fun addAuditTypesPrimitive() {
        AuditType.addType(AuditType(String::class, "String", StringSerializer) as AuditType<Any>)
        AuditType.addType(AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>)
        AuditType.addType(AuditType(Long::class, "Long", LongSerializer) as AuditType<Any>)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun addAuditTypePrimitive(auditDao: AuditDao) {
        auditDao.addTypeInDbModel(AuditType(String::class, "String", StringSerializer) as AuditType<Any>)
        auditDao.addTypeInDbModel(AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>)
        auditDao.addTypeInDbModel(AuditType(Long::class, "Long", LongSerializer) as AuditType<Any>)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun addInformationTypesPrimitive() {
        InformationType.addType(InformationType(IdPresenter,
                PropertyLoader.loadProperty("IdColumn") ?: "Id", InformationType.InformationInnerType.Long) as InformationType<Any>)
        InformationType.addType(InformationType(VersionPresenter,
                PropertyLoader.loadProperty("VersionColumn") ?: "Version", InformationType.InformationInnerType.ULong) as InformationType<Any>)
        InformationType.addType(InformationType(TimeStampPresenter,
                PropertyLoader.loadProperty("TimeStampColumn") ?: "TimeStamp",
                InformationType.InformationInnerType.Long) as InformationType<Any>)
    }
}
