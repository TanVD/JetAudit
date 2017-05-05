package Clickhouse.AuditDao.Saving

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.`is`
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.queries.or
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.AuditType.TypesResolution.addType
import tanvd.audit.model.external.types.AuditType.TypesResolution.resolveType
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.*

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

        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)

        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val typeTestClassSecond = AuditType(TestClassSecond::class, "TestClassSecond", TestClassSecond) as AuditType<Any>
        addType(typeTestClassSecond)
        auditDao!!.addTypeInDbModel(typeTestClassSecond)

    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
        currentId = 0
    }

    @Test
    fun saveRecord_primitiveTypes_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = AuditRecordInternal(getSamplePrimitiveArrayObjects(), getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(Int::class equal 123, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecords_PrimitiveTypes_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSamplePrimitiveArrayObjects(), getSampleInformation())
        val auditRecordSecondOriginal = AuditRecordInternal(getSamplePrimitiveArrayObjects(postFixFirst = "1"), getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(Int::class equal 123, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun saveRecord_NonPrimitiveTypes_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = AuditRecordInternal(getSampleNonPrimitiveArrayObjects(), getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TestClassFirst::class equal TestClassFirst(), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecords_NonPrimitiveTypes_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleNonPrimitiveArrayObjects(), getSampleInformation())
        val arrayObjectsSecond = getSampleNonPrimitiveArrayObjects()
        arrayObjectsSecond.addAll(getSamplePrimitiveArrayObjects())
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassSecond::class equal TestClassSecond(), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_LongInformation_loadRecordsReturnSavedRecord() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(1))

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TimeStampPresenter equal 1, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_LongInformation_loadRecordsReturnSavedRecords() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(1))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((TimeStampPresenter equal 1) or (TimeStampPresenter equal 2),
                QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_BooleanInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(BooleanPresenter, "BooleanPresenter", InformationType.InformationInnerType.Boolean) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val information = getSampleInformation(1)
        information.add(InformationObject(true, BooleanPresenter))

        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(BooleanPresenter `is` true, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_BooleanInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(BooleanPresenter, "BooleanPresenter", InformationType.InformationInnerType.Boolean) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject(true, BooleanPresenter))
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject(true, BooleanPresenter))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanPresenter `is` true, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecord_StringInformation_loadRecordsReturnSavedRecord() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringPresenter, "StringPresenter", InformationType.InformationInnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val information = getSampleInformation(1)
        information.add(InformationObject("string", StringPresenter))

        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), information)

        auditDao!!.saveRecord(auditRecordFirstOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecords_StringInformation_loadRecordsReturnSavedRecords() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringPresenter, "StringPresenter", InformationType.InformationInnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val informationFirst = getSampleInformation(1)
        informationFirst.add(InformationObject("string", StringPresenter))
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), informationFirst)

        val informationSecond = getSampleInformation(2)
        informationSecond.add(InformationObject("string", StringPresenter))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), informationSecond)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun saveRecords_ReplacingRow_NewRowReturnedById() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleNonPrimitiveArrayObjects(), getSampleInformation(0, 1, 0))
        auditDao!!.saveRecord(auditRecordFirstOriginal)
        val arrayObjectsSecond = getSampleNonPrimitiveArrayObjects()
        arrayObjectsSecond.addAll(getSamplePrimitiveArrayObjects())
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation(0, 1, 1))

        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(IdPresenter equal 0, QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordSecondOriginal))
    }


    private fun getSamplePrimitiveArrayObjects(postFixFirst: String = "", postFixSecond: String = ""):
            List<Pair<AuditType<Any>, String>> {
        return arrayListOf(Pair(resolveType(String::class), "string" + postFixFirst),
                Pair(resolveType(Int::class), "123" + postFixSecond))
    }

    private fun getSampleNonPrimitiveArrayObjects(): MutableList<Pair<AuditType<Any>, String>> {
        return arrayListOf(
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(resolveType(TestClassSecond::class), "TestClassSecondId"))
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
    }

    private fun getSampleInformation(timeStamp: Long): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2)
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long = 2): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version)
    }

}
