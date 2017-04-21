package Clickhouse.AuditDao.Saving

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

internal class AuditDaoSavingClickhouse() {

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

    class TestClassSecond {
        companion object serializer : AuditSerializer<TestClassSecond> {
            override fun display(value: TestClassSecond): String {
                return "TestClassSecondDisplay"
            }

            override fun deserialize(serializedString: String): TestClassSecond {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassSecond): String {
                return "TestClassSecondId"
            }

        }
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val typeString = AuditType(String::class, "String", StringSerializer) as AuditType<Any>
        addType(typeString)
        auditDao!!.addTypeInDbModel(typeString)

        val typeInt = AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>
        addType(typeInt)
        auditDao!!.addTypeInDbModel(typeInt)

        val typeTestClassSecond = AuditType(TestClassSecond::class, "TestClassSecond", TestClassSecond) as AuditType<Any>
        addType(typeTestClassSecond)
        auditDao!!.addTypeInDbModel(typeTestClassSecond)

    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
    }

    @Test
    fun saveRow_PrimitiveTypes_loadedNormally() {
        val arrayObjects = arrayListOf(Pair(resolveType(String::class), "string"), Pair(resolveType(Int::class), "123"))
        val auditRecordOriginal = AuditRecordInternal(arrayObjects, 127)

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(Int::class equal 123, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRows_PrimitiveTypes_loadedNormally() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "string"),
                Pair(resolveType(Int::class), "123"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "string2"),
                Pair(resolveType(Int::class), "123"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, 254)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(Int::class equal 123, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun saveRow_NonPrimitiveTypes_loadedNormally() {
        val arrayObjects = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"))
        val auditRecordOriginal = AuditRecordInternal(arrayObjects, 127)

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TestClassFirst::class equal TestClassFirst(), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRows_NonPrimitiveTypes_loadedNormally() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"),
                Pair(resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, 254)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassSecond::class equal TestClassSecond(), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

}
