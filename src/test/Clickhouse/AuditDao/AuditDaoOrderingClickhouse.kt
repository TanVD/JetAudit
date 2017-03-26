package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import tanvd.audit.model.QueryParameters
import tanvd.audit.model.QueryParameters.OrderByParameters.Order.ASC
import tanvd.audit.model.QueryParameters.OrderByParameters.Order.DESC
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer

internal class AuditDaoOrderingClickhouse() {

    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        val typeString = AuditType(String::class, "String", StringSerializer) as AuditType<Any>
        AuditType.addType(typeString)
        auditDao!!.addTypeInDbModel(typeString)

        val typeInt = AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>
        AuditType.addType(typeInt)
        auditDao!!.addTypeInDbModel(typeInt)

    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable("Audit")
        AuditType.clearTypes()
    }

    @Test
    fun loadRows_AscendingByIntsIds_AscendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 255)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(false, klasses = Pair(Int::class, ASC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_DescendingByIntsIds_DescendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 255)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(false, klasses = Pair(Int::class, DESC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_AscendingByTimeStamp_AscendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 127)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, ASC)
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_DescendingByTimeStamp_DescendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 127)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, DESC)
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_AscendingByTimeStampEqualsAndIntsIds_AscendingByStrings() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, ASC, Pair(Int::class, ASC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
    }

    @Test
    fun loadRows_DescendingByTimeStampEqualsAndIntsIds_DescendingByStrings() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, DESC, Pair(Int::class, DESC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
    }

    @Test
    fun loadRows_AscendingByTimeStampAndIntsIds_AscendingByTimeStamp() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, ASC, Pair(Int::class, ASC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_DescendingByTimeStampAndIntsIds_DescendingByTimeStamp() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, DESC, Pair(Int::class, DESC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)


    }

    @Test
    fun loadRows_DescendingByTimeStampAndAscendingIntsIds_DescendingByTimeStamp() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, DESC, Pair(Int::class, ASC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
    }

    @Test
    fun loadRows_DescendingByEqualsTimeStampAndAscendingIntsIds_AscendingByStrings() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, DESC, Pair(Int::class, ASC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementDifferent_AscendingByFirstElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "121"),
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(false, klasses = Pair(Int::class, ASC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementDifferent_DescendingByFirstElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "121"),
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(false, klasses = Pair(Int::class, DESC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementEqual_AscendingBySecondElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(false, klasses = Pair(Int::class, ASC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementEqual_DescendingBySecondElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "111"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "129"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 25)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(false, klasses = Pair(Int::class, DESC))
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
    }
}
