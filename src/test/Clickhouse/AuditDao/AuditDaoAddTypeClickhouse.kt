package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditSerializer
import tanvd.audit.model.AuditType
import tanvd.audit.model.AuditType.TypesResolution.addType
import tanvd.audit.model.AuditType.TypesResolution.resolveType
import tanvd.audit.model.QueryParameters
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer

internal class AuditDaoAddTypeClickhouse {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    class TestClassFirst {
        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun deserialize(serializedString: String): TestClassFirst {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassFirst): String {
                throw UnsupportedOperationException("not implemented")
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
        auditDao!!.dropTable("Audit")
        AuditType.clearTypes()
    }

    @Test
    fun saveRowBefore_addType_loadedNormally() {
        val arrayObjects = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordOriginal = AuditRecord(arrayObjects, 127)
        auditDao!!.saveRecord(auditRecordOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(Int::class), "27", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordOriginal.unixTimeStamp)
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyNotNew() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 123)
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 127)
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "Who is ", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyNew() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 123)
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 127)
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(TestClassFirst::class), "TestClassFirstId", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyBoth() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 123)
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 127)
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(Int::class), "27", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertTrue(recordsLoaded.map { it.objects }.containsAll(listOf(arrayObjectsFirst, arrayObjectsSecond)))
        Assert.assertTrue(recordsLoaded.map { it.unixTimeStamp }.containsAll(arrayListOf(123, 127)))
    }
}
