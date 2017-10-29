package utils

import Clickhouse.AuditDao.Loading.Information.InformationBooleanQueriesTest
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType

internal object TestUtil {
    fun create() : AuditDaoClickhouseImpl {
        AuditTable.db = TestDatabase

        try {
            AuditTable.drop()
        } catch (e: Exception) {}

        AuditTable.resetColumns()

        val auditDao = AuditDaoClickhouseImpl()

        addObjectTypePrimitives()
        addAuditTypePrimitive(auditDao)
        addInformationTypesPrimitive()
        return auditDao
    }

    fun drop() {
        AuditTable.db = TestDatabase
        try {
            AuditTable.drop()
        } catch (e: Exception) {}
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
    internal fun addAuditTypePrimitive(auditDao: AuditDao) {
        auditDao.addTypeInDbModel(ObjectType(String::class, StringPresenter) as ObjectType<Any>)
        auditDao.addTypeInDbModel(ObjectType(Int::class, IntPresenter) as ObjectType<Any>)
        auditDao.addTypeInDbModel(ObjectType(Long::class, LongPresenter) as ObjectType<Any>)
    }

    internal fun addInformationTypesPrimitive() {
        InformationType.addType(IdType)
        InformationType.addType(VersionType)
        InformationType.addType(TimeStampType)
        InformationType.addType(DateType)
        InformationType.addType(IsDeletedType)
    }
}