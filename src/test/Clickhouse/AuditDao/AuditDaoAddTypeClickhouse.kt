package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.AuditSerializer
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.external.AuditType.TypesResolution.addType
import tanvd.audit.model.external.AuditType.TypesResolution.resolveType
import tanvd.audit.model.external.QueryParameters
import tanvd.audit.model.external.equal
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer
import utils.TypeUtils

internal class AuditDaoAddTypeClickhouse {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    class TestClassFirst {
        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun display(value: TestClassFirst): String {
                return "TestClassFirstDisplay"
            }

            override fun deserialize(serializedString: String): TestClassFirst {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassFirst): String {
                return "TestClassFirstId"
            }

        }
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        val typeString = AuditType(String::class, "String", StringSerializer) as AuditType<Any>
        addType(typeString)
        auditDao!!.addTypeInDbModel(typeString)

        val typeInt = AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>
        addType(typeInt)
        auditDao!!.addTypeInDbModel(typeInt)
    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
    }

    @Test
    fun saveRowBefore_addType_loadedNormally() {
        val arrayObjects = arrayListOf(Pair(resolveType(String::class), "string"))
        val auditRecordOriginal = AuditRecordInternal(arrayObjects, 127)
        auditDao!!.saveRecord(auditRecordOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyNotNew() {
        val arrayObjectsFirst = arrayListOf(Pair(resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, 127)
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(Pair(resolveType(TestClassFirst::class), "TestClassFirstId"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, 127)
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", QueryParameters())

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyNew() {
        val arrayObjectsFirst = arrayListOf(Pair(resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, 127)
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(Pair(resolveType(TestClassFirst::class), "TestClassFirstId"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, 127)
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(TestClassFirst::class equal TestClassFirst(), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyBoth() {
        val arrayObjectsFirst = arrayListOf(Pair(resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, 127)
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "string"),
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, 127)
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }
}
