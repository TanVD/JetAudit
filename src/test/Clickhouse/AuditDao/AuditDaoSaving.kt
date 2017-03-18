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

internal class AuditDaoSaving() {

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

        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        addType(typeTestClassFirst)
        auditDao!!.addType(typeTestClassFirst)

        val typeString = AuditType(String::class, "String", StringSerializer) as AuditType<Any>
        addType(typeString)
        auditDao!!.addType(typeString)

        val typeInt = AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>
        addType(typeInt)
        auditDao!!.addType(typeInt)

        val typeTestClassSecond = AuditType(TestClassSecond::class, "TestClassSecond", TestClassSecond) as AuditType<Any>
        addType(typeTestClassSecond)
        auditDao!!.addType(typeTestClassSecond)

    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable("Audit")
        AuditDaoClickhouseImpl.types.clear()
        AuditType.auditTypesByClass.clear()
        AuditType.auditTypesByCode.clear()
    }

    @Test
    fun saveRow_PrimitiveTypes_loadedNormally() {
        val arrayObjects = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordOriginal = AuditRecord(arrayObjects)
        auditDao!!.saveRow(auditRecordOriginal)
        val recordsLoaded = auditDao!!.loadRow(resolveType(Int::class), "27")
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordOriginal.objects)
    }

    @Test
    fun saveRows_PrimitiveTypes_loadedNormallyOne() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "Oh god "),
                Pair(resolveType(Int::class), "what have i done "),
                Pair(resolveType(String::class), "i came to berlin "),
                Pair(resolveType(String::class), "in the army?"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRows(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRow(resolveType(Int::class), "27")
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
    }

    @Test
    fun saveRows_PrimitiveTypes_loadedNormallyBoth() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), "in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "Oh god "),
                Pair(resolveType(String::class), "what have i done "),
                Pair(resolveType(String::class), "i came to berlin "),
                Pair(resolveType(String::class), "in the army?"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRows(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRow(resolveType(String::class), "in the army?")
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertTrue(recordsLoaded.map { it.objects }.containsAll(listOf(arrayObjectsFirst, arrayObjectsSecond)))
    }

    @Test
    fun saveRows_PrimitiveTypes_loadedNone() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), "in the army?"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "Oh god "),
                Pair(resolveType(String::class), "what have i done "),
                Pair(resolveType(String::class), "i came to berlin "),
                Pair(resolveType(String::class), "in the army?"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRows(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRow(resolveType(String::class), "Bad String")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun saveRow_NonPrimitiveTypes_loadedNormallyOne() {
        val arrayObjects = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"))
        val auditRecordOriginal = AuditRecord(arrayObjects)
        auditDao!!.saveRow(auditRecordOriginal)
        val recordsLoaded = auditDao!!.loadRow(resolveType(TestClassFirst::class), "TestClassFirstId")
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordOriginal.objects)
    }

    @Test
    fun saveRows_NonPrimitiveTypes_loadedNormallyOne() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "It is Fine"),
                Pair(resolveType(String::class), "Calm down"),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"),
                Pair(resolveType(String::class), "you"),
                Pair(resolveType(String::class), "have been called"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRows(listOf(auditRecordFirstOriginal,auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRow(resolveType(TestClassFirst::class), "TestClassFirstId")
        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
    }

    @Test
    fun saveRows_NonPrimitiveTypes_loadedNormallyBoth() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "It is Fine"),
                Pair(resolveType(String::class), "Calm down"),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"),
                Pair(resolveType(String::class), "you"),
                Pair(resolveType(String::class), "have been called"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRows(listOf(auditRecordFirstOriginal,auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRow(resolveType(String::class), "have been called")
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertTrue(recordsLoaded.map { it.objects }.containsAll(listOf(arrayObjectsFirst, arrayObjectsSecond)))
    }

    @Test
    fun saveRows_NonPrimitiveTypes_loadedNormallyNone() {
        val arrayObjectsFirst = arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(String::class), "have been called"),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " times by "),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst)
        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "It is Fine"),
                Pair(resolveType(String::class), "Calm down"),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"),
                Pair(resolveType(String::class), "you"),
                Pair(resolveType(String::class), "have been called"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond)
        auditDao!!.saveRows(listOf(auditRecordFirstOriginal,auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRow(resolveType(String::class), "Bad String")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

}
