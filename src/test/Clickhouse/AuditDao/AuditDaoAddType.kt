package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditSerializer
import tanvd.audit.model.AuditType
import tanvd.audit.model.AuditType.TypesResolution.addType
import tanvd.audit.model.AuditType.TypesResolution.resolveType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer
import java.sql.DriverManager


internal class AuditDaoAddType() {

    companion object {
        var auditDao : AuditDao? = null
    }

    class TestClassFirst(){
        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun deserialize(serializedString: String): TestClassFirst {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassFirst): String {
                throw UnsupportedOperationException("not implemented")
            }

        }
    }

    class TestClassSecond() {
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
        val rawConnection = DriverManager.getConnection("jdbc:clickhouse://localhost:8123/example", "default", "")
        auditDao = AuditDaoClickhouseImpl(rawConnection)



        val typeString = AuditType(String::class, "String", StringSerializer) as AuditType<Any>
        addType(typeString)
        auditDao!!.addType(typeString)

        val typeInt = AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>
        addType(typeInt)
        auditDao!!.addType(typeInt)
    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable("Audit")
        AuditDaoClickhouseImpl.types.clear()
        AuditType.auditTypesByClass.clear()
        AuditType.auditTypesByCode.clear()
    }

    @Test
    fun saveRowBefore_addType_loadedNormally() {
        val arrayObjects = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordOriginal = AuditRecord(arrayObjects)
        auditDao!!.saveRow(auditRecordOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addType(typeTestClassFirst)

        val recordsLoaded = auditDao!!.loadRow(AuditType.resolveType(Int::class), "27")
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordOriginal.objects)
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyNotNew() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        auditDao!!.saveRow(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addType(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRow(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRow(AuditType.resolveType(String::class), "Who is ")
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyNew() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        auditDao!!.saveRow(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addType(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRow(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRow(AuditType.resolveType(TestClassFirst::class), "TestClassFirstId")
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
    }

    @Test
    fun saveRowBefore_addTypeAndSaveWithNewType_loadedNormallyBoth() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        auditDao!!.saveRow(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addType(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRow(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRow(AuditType.resolveType(Int::class), "27")
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertTrue(recordsLoaded.map{it.objects}.containsAll(listOf(arrayObjectsFirst, arrayObjectsSecond)))
    }
}
