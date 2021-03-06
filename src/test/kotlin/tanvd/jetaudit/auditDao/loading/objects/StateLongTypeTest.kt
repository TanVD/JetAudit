package tanvd.jetaudit.auditDao.loading.objects

import org.junit.*
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.*
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal

@Suppress("UNCHECKED_CAST")
internal class StateLongTypeTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse? = null
    }

    val type = ObjectType(TestClassLong::class, TestClassLongPresenter) as ObjectType<Any>

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
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id equal 1)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id equal 0)
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //List
    @Test
    fun loadRow_LoadByInList_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id inList listOf(1L))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByInList_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id inList listOf(0L))
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //Number
    @Test
    fun loadRow_LoadByLess_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id less 2)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLess_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id less 1)
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByMore_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id more 0)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByMore_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(TestClassLong(1), information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TestClassLongPresenter.id more 1)
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }
}

