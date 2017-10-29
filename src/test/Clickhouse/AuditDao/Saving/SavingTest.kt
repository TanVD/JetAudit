package Clickhouse.AuditDao.Saving

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.aorm.Column
import tanvd.aorm.DbType
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.IdType
import tanvd.audit.model.external.presenters.LongPresenter
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.external.types.objects.ObjectType.TypesResolution.addType
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class SavingTest {

    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
        var currentId = 0L
    }

    val typeTestClassLong = ObjectType(TestClassLong::class, TestClassLongPresenter) as ObjectType<Any>
    val typeTestClassString = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>


    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()

        addType(typeTestClassLong)
        auditDao!!.addTypeInDbModel(typeTestClassLong)

        addType(typeTestClassString)
        auditDao!!.addTypeInDbModel(typeTestClassString)

    }

    @AfterMethod
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    @Test
    fun saveRecord_primitiveTypes_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = getRecordInternal(123L, "string", information = getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(LongPresenter.value equal 123)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecords_PrimitiveTypes_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = getRecordInternal(123L, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(123L, "string1", information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(LongPresenter.value equal 123)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun saveRecord_NonPrimitiveTypes_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = getRecordInternal(TestClassLong(1), TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id equal 1)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecords_NonPrimitiveTypes_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), TestClassString("string"), information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(TestClassLong(1), TestClassString("string"), 123L, information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string")
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_LongInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(LongInf)
        auditDao!!.addInformationInDbModel(LongInf)

        val information = getSampleInformation(1)
        information.add(InformationObject(0, LongInf))

        val auditRecordFirstOriginal = getRecordInternal(information = information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(LongInf equal 0)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_LongInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(LongInf)
        auditDao!!.addInformationInDbModel(LongInf)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject(0, LongInf))
        val auditRecordFirstOriginal = getRecordInternal(information = informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject(0, LongInf))
        val auditRecordSecondOriginal = getRecordInternal(information = informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(LongInf equal 0)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }


    @Test
    fun saveRecord_DateInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(DateInf)
        auditDao!!.addInformationInDbModel(DateInf)

        val information = getSampleInformation(1)
        information.add(InformationObject(getDate("2000-01-01"), DateInf))

        val auditRecordFirstOriginal = getRecordInternal(information = information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(DateInf equal getDate("2000-01-01"))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_DateInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(DateInf)
        auditDao!!.addInformationInDbModel(DateInf)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject(getDate("2000-01-01"), DateInf))
        val auditRecordFirstOriginal = getRecordInternal(information = informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject(getDate("2000-01-01"), DateInf))
        val auditRecordSecondOriginal = getRecordInternal(information = informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateInf equal getDate("2000-01-01"))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }


    @Test
    fun saveRecord_DateTimeInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(DateTimeInf)
        auditDao!!.addInformationInDbModel(DateTimeInf)

        val information = getSampleInformation(1)
        information.add(InformationObject(getDateTime("2000-01-01 12:00:00"), DateTimeInf))

        val auditRecordFirstOriginal = getRecordInternal(information = information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf equal getDateTime("2000-01-01 12:00:00"))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_DateTimeInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(DateTimeInf)
        auditDao!!.addInformationInDbModel(DateTimeInf)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject(getDateTime("2000-01-01 12:00:00"), DateTimeInf))
        val auditRecordFirstOriginal = getRecordInternal(information = informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject(getDateTime("2000-01-01 12:00:00"), DateTimeInf))
        val auditRecordSecondOriginal = getRecordInternal(information = informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf equal getDateTime("2000-01-01 12:00:00"))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }


    @Test
    fun saveRecord_BooleanInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(BooleanInf)
        auditDao!!.addInformationInDbModel(BooleanInf)

        val information = getSampleInformation(1)
        information.add(InformationObject(true, BooleanInf))

        val auditRecordFirstOriginal = getRecordInternal(information = information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(BooleanInf equal true)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_BooleanInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(BooleanInf)
        auditDao!!.addInformationInDbModel(BooleanInf)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject(true, BooleanInf))
        val auditRecordFirstOriginal = getRecordInternal(information = informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject(true, BooleanInf))
        val auditRecordSecondOriginal = getRecordInternal(information = informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInf equal true)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_StringInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(StringInf)
        auditDao!!.addInformationInDbModel(StringInf)

        val information = getSampleInformation(1)
        information.add(InformationObject("string", StringInf))

        val auditRecordFirstOriginal = getRecordInternal(information = information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringInf equal "string")
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_StringInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(StringInf)
        auditDao!!.addInformationInDbModel(StringInf)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject("string", StringInf))
        val auditRecordFirstOriginal = getRecordInternal(information = informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject("string", StringInf))
        val auditRecordSecondOriginal = getRecordInternal(information = informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf equal "string")
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecords_ReplacingRow_NewRowReturnedById() {
        val auditRecordFirstOriginal = getRecordInternal(456, TestClassLong(1), information = getSampleInformation(0, 1, 0))
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val auditRecordSecondOriginal = getRecordInternal(123, TestClassString("string"), information = getSampleInformation(0, 1, 1))
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(IdType equal 0)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordSecondOriginal))
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(timeStamp: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long = 2): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }

}
