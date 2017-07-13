package Clickhouse.AuditDao.Saving

import Clickhouse.AuditDao.Loading.Information.InformationBoolean
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.LongPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.queries.or
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.InnerType
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

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {

        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        AuditDao.credentials = DbUtils.getCredentials()
        auditDao = AuditDao.getDao() as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)

        val typeTestClassLong = ObjectType(TestClassLong::class, TestClassLongPresenter) as ObjectType<Any>
        addType(typeTestClassLong)
        auditDao!!.addTypeInDbModel(typeTestClassLong)

        val typeTestClassString = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>
        addType(typeTestClassString)
        auditDao!!.addTypeInDbModel(typeTestClassString)

    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
        currentId = 0
    }

    @Test
    fun saveRecord_primitiveTypes_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = getRecordInternal(123L, "string", information = getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(LongPresenter.value equal 123, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecords_PrimitiveTypes_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = getRecordInternal(123L, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(123L, "string1", information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(LongPresenter.value equal 123, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun saveRecord_NonPrimitiveTypes_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = getRecordInternal(TestClassLong(1), TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id equal 1, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecords_NonPrimitiveTypes_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), TestClassString("string"), information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(TestClassLong(1), TestClassString("string"), 123L, information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_LongInformation_loadRecordsReturnSavedRecord() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TimeStampPresenter equal 1, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_LongInformation_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(2))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((TimeStampPresenter equal 1) or (TimeStampPresenter equal 2),
                QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_BooleanInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(BooleanInfPresenter, InnerType.Boolean) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val information = getSampleInformation(1)
        information.add(InformationObject(true, BooleanInfPresenter))

        val auditRecordFirstOriginal = getRecordInternal(information = information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(BooleanInfPresenter equal true, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_BooleanInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(BooleanInfPresenter, InnerType.Boolean) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject(true, BooleanInfPresenter))
        val auditRecordFirstOriginal = getRecordInternal(information = informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject(true, BooleanInfPresenter))
        val auditRecordSecondOriginal = getRecordInternal(information = informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInfPresenter equal true, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_StringInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringInfPresenter, InnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val information = getSampleInformation(1)
        information.add(InformationObject("string", StringInfPresenter))

        val auditRecordFirstOriginal = getRecordInternal(information = information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringInfPresenter equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_StringInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringInfPresenter, InnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject("string", StringInfPresenter))
        val auditRecordFirstOriginal = getRecordInternal(information = informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject("string", StringInfPresenter))
        val auditRecordSecondOriginal = getRecordInternal(information = informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInfPresenter equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecords_ReplacingRow_NewRowReturnedById() {
        val auditRecordFirstOriginal = getRecordInternal(456, TestClassLong(1), information = getSampleInformation(0, 1, 0))
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val auditRecordSecondOriginal = getRecordInternal(123, TestClassString("string"), information = getSampleInformation(0, 1, 1))
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(IdPresenter equal 0, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordSecondOriginal))
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(timeStamp: Long): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long = 2): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }

}
