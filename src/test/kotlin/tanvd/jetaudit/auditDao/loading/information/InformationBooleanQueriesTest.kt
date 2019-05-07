package tanvd.jetaudit.auditDao.loading.information

import org.junit.*
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.inList
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal

internal class InformationBooleanQueriesTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse? = null
    }

    @Before
    fun createAll() {
        auditDao = TestUtil.create()

        auditDao!!.addInformationInDbModel(BooleanInf)
        InformationType.addType(BooleanInf)
    }

    @After
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

//Equality

    @Test
    fun loadRow_LoadByIs_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInf equal true)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIs_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInf equal false)
        Assert.assertEquals(recordsLoaded.size, 0)
    }

//List

    @Test
    fun loadRow_LoadByInList_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInf inList listOf(true))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByInList_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInf inList listOf(false))
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(value: Boolean): LinkedHashSet<InformationObject<*>> {
        val information = InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
        (information).add(InformationObject(value, BooleanInf))
        return information

    }
}
