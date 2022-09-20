package tanvd.jetaudit.auditDao.loading.objects

import org.junit.*
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.*
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal

@Suppress("UNCHECKED_CAST")
internal class StateStringTypeTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse? = null
    }

    val type = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>

    @Before
    fun createAll() {
        auditDao = TestUtil.create()

        ObjectType.addType(type)
        auditDao!!.addTypeInDbModel(type)
    }

    @After
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    //Equality
    @Test
    fun loadRow_LoadByEqual_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "error")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //List
    @Test
    fun loadRow_LoadByInList_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id inList listOf("string"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByInList_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id inList listOf("error"))
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //String
    @Test
    fun loadRow_LoadByLike_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id like "str%")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLike_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id like "s_")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByILike_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id iLike "sTr%")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByILike_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id iLike "s_")
        Assert.assertEquals(recordsLoaded.size, 0)
    }


    @Test
    fun loadRow_LoadByRegExp_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id regex "st")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByRegExp_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id regex "zo")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }
}
