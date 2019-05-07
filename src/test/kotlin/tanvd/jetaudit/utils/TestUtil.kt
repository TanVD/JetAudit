package tanvd.jetaudit.utils

import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.implementation.clickhouse.aorm.*
import tanvd.jetaudit.model.external.presenters.*
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.model.external.types.objects.ObjectType

internal object TestUtil {

    init {
        AuditDatabase.init(PropertyLoader[Conf.AUDIT_DATABASE], DbUtils.getDataSource())
        val randomPostfix = RandomGenerator.next()
        AuditTable.name += randomPostfix.toString()
    }

    fun create(): AuditDaoClickhouse = withAuditDatabase {
        try {
            AuditTable.drop()
        } catch (e: Exception) {
        }
        AuditTable.resetColumns()
        AuditTable.create()

        val auditDao = AuditDaoClickhouse()

        addObjectTypePrimitives()
        addAuditTypePrimitive(auditDao)
        addInformationTypesPrimitive()
        auditDao
    }

    fun drop() {
        try {
            withAuditDatabase { AuditTable.drop() }
        } catch (e: Exception) {
        }
        clearTypes()
    }


    fun clearTypes() {
        ObjectType.clearTypes()
        InformationType.clearTypes()
    }


    @Suppress("UNCHECKED_CAST")
    internal fun addObjectTypePrimitives() {
        ObjectType.addType(ObjectType(String::class, StringPresenter) as ObjectType<Any>)
        ObjectType.addType(ObjectType(Int::class, IntPresenter) as ObjectType<Any>)
        ObjectType.addType(ObjectType(Long::class, LongPresenter) as ObjectType<Any>)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun addAuditTypePrimitive(auditDao: AuditDaoClickhouse) {
        auditDao.addTypeInDbModel(ObjectType(String::class, StringPresenter) as ObjectType<Any>)
        auditDao.addTypeInDbModel(ObjectType(Int::class, IntPresenter) as ObjectType<Any>)
        auditDao.addTypeInDbModel(ObjectType(Long::class, LongPresenter) as ObjectType<Any>)
    }

    private fun addInformationTypesPrimitive() {
        InformationType.addType(IdType)
        InformationType.addType(VersionType)
        InformationType.addType(TimeStampType)
        InformationType.addType(DateType)
        InformationType.addType(IsDeletedType)
    }
}
