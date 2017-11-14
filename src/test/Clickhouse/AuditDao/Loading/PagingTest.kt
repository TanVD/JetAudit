//package Clickhouse.AuditDao.Loading
//
//import org.testng.Assert
//import org.testng.annotations.AfterMethod
//import org.testng.annotations.BeforeMethod
//import org.testng.annotations.Test
//import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
//import tanvd.audit.implementation.dao.AuditDao
//import tanvd.audit.model.external.presenters.StringPresenter
//import tanvd.audit.model.external.presenters.TimeStampType
//import tanvd.audit.model.external.queries.EqualityCondition
//import tanvd.audit.model.external.queries.QueryEqualityTypeLeaf
//import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order.ASC
//import tanvd.audit.model.external.records.InformationObject
//import tanvd.audit.model.external.types.InnerType
//import utils.DbUtils
//import utils.InformationUtils
//import utils.SamplesGenerator
//import utils.SamplesGenerator.getRecordInternal
//import utils.TypeUtils
//
//internal class PagingTest {
//
//    companion object {
//        var currentId = 0L
//        var auditDao: AuditDaoClickhouse? = null
//    }
//
//    @BeforeMethod
//    @Suppress("UNCHECKED_CAST")
//    fun createAll() {
//        TypeUtils.addObjectTypePrimitives()
//        TypeUtils.addInformationTypesPrimitive()
//
//        AuditDao.credentials = DbUtils.getCredentials()
//        auditDao = AuditDao.getDao() as AuditDaoClickhouse
//
//        TypeUtils.addAuditTypePrimitive(auditDao!!)
//    }
//
//    @AfterMethod
//    fun clearAll() {
//        auditDao!!.dropTable(AuditDaoClickhouse.auditTable)
//        TypeUtils.clearTypes()
//        currentId = 0
//    }
//
//    @Test
//    fun loadRows_limitOneFromZero_gotFirst() {
//        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation(1))
//        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation(2))
//        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//        val parameters = QueryParameters()
//        parameters.setLimits(0, 1)
//        parameters.setInformationOrder(TimeStampType to ASC)
//        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
//
//        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
//    }
//
//    @Test
//    fun loadRows_limitOneFromFirst_gotSecond() {
//        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation(1))
//        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation(2))
//        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//        val parameters = QueryParameters()
//        parameters.setLimits(1, 1)
//        parameters.setInformationOrder(TimeStampType to ASC)
//        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
//
//        Assert.assertEquals(recordsLoaded.size, 1)
//        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordSecondOriginal))
//    }
//
//    @Test
//    fun loadRows_limitTwoFromZero_gotBoth() {
//        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation(1))
//        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation(2))
//        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//        val parameters = QueryParameters()
//        parameters.setLimits(0, 2)
//        parameters.setInformationOrder(TimeStampType to ASC)
//        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
//
//        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//    }
//
//    @Test
//    fun countRows_countNoSavedRows_gotRightNumber() {
//        val count = auditDao!!.countRecords(QueryEqualityTypeLeaf(EqualityCondition.equal, "string", InnerType.String,
//                StringPresenter.value))
//        Assert.assertEquals(count, 0)
//    }
//
//    @Test
//    fun countRows_countTwoSavedRows_gotRightNumber() {
//        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation())
//        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation())
//        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
//
//        val count = auditDao!!.countRecords(StringPresenter.value equal "string")
//        Assert.assertEquals(count, 2)
//    }
//
//    private fun getSampleInformation(): MutableSet<InformationObject<*>> {
//        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
//    }
//
//    private fun getSampleInformation(timeStamp: Long): MutableSet<InformationObject<*>> {
//        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
//    }
//
//}
//
