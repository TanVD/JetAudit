package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditSerializer
import tanvd.audit.model.AuditType
import tanvd.audit.model.AuditType.TypesResolution.addType
import tanvd.audit.model.AuditType.TypesResolution.resolveType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer

internal class AuditDaoSavingClickhouse() {

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
        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "")

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
        auditDao!!.dropTable("Audit")
        AuditType.clearTypes()
    }

    @Test
    fun saveRow_PrimitiveTypes_loadedNormally() {
        val arrayObjects = arrayListOf(
                Pair(resolveType(String::class), "Who is "),
                Pair(resolveType(Int::class), "27"),
                Pair(resolveType(String::class), " in the army?"))
        val auditRecordOriginal = AuditRecord(arrayObjects)
        auditDao!!.saveRecord(auditRecordOriginal)
        val recordsLoaded = auditDao!!.loadRecords(resolveType(Int::class), "27")
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
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRecords(resolveType(Int::class), "27")
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
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRecords(resolveType(String::class), "in the army?")
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
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRecords(resolveType(String::class), "Bad String")
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
        auditDao!!.saveRecord(auditRecordOriginal)
        val recordsLoaded = auditDao!!.loadRecords(resolveType(TestClassFirst::class), "TestClassFirstId")
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
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal,auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRecords(resolveType(TestClassFirst::class), "TestClassFirstId")
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
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal,auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRecords(resolveType(String::class), "have been called")
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
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal,auditRecordSecondOriginal))
        val recordsLoaded = auditDao!!.loadRecords(resolveType(String::class), "Bad String")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

}
