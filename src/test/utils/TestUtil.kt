package utils

import tanvd.aorm.Database
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.utils.RandomGenerator

internal object TestUtil {

    init {
        AuditTable.init(DbUtils.getDataSource())
        val randomPostfix = RandomGenerator.next()
        AuditTable().name += randomPostfix.toString()
    }

    fun create(): AuditDaoClickhouse {

        try {
            AuditTable().drop()
        } catch (e: Exception) {
        }
        AuditTable().resetColumns()
        AuditTable().create()

        val auditDao = AuditDaoClickhouse()

        addObjectTypePrimitives()
        addAuditTypePrimitive(auditDao)
        addInformationTypesPrimitive()
        return auditDao
    }

    fun drop() {
        try {
            AuditTable().drop()
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

    internal fun addInformationTypesPrimitive() {
        InformationType.addType(IdType)
        InformationType.addType(VersionType)
        InformationType.addType(TimeStampType)
        InformationType.addType(DateType)
    }
}

val TestDatabase = Database("testDb", DbUtils.getDataSource())
