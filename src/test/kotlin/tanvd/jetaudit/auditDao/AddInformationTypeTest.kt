package tanvd.jetaudit.auditDao

import org.junit.*
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.presenters.IdType
import tanvd.jetaudit.model.external.presenters.TimeStampType
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal

internal class AddInformationTypeTest {


    companion object {
        var auditDao: AuditDaoClickhouse? = null
        var currentId = 0L

    }

    @Before
    fun createAll() {
        auditDao = TestUtil.create()
    }

    @After
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    @Test
    fun saveRecordBefore_addType_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordOriginal)

        addStringInformation()

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType equal 1)

        auditRecordOriginal.information.add(InformationObject(StringInf.default(),
                StringInf))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNotNew() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = getRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(IdType equal 0)

        auditRecordFirstOriginal.information.add(InformationObject(StringInf.default(),
                StringInf))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNew() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = getRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringInf equal "string")

        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedBoth() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = getRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType equal 1)

        auditRecordFirstOriginal.information.add(InformationObject(StringInf.default(), StringInf))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleWithStringInformation(): LinkedHashSet<InformationObject<*>> {
        val information = getSampleInformation()
        information.add(InformationObject("string", InformationType.resolveType("StringInfColumn")))
        return information
    }

    private fun addStringInformation() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(StringInf)
        auditDao!!.addInformationInDbModel(StringInf)
    }
}

