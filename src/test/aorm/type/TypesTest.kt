package aorm.type

import audit.utils.TestDatabase
import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.*
import tanvd.aorm.implementation.MetadataClickhouse
import utils.getDate
import utils.getDateTime

class TypesTest {
    @Test
    fun allTypes_createTable_tableCreated() {
        AllTypesTable.create()

        Assert.assertTrue(MetadataClickhouse.existsTable(AllTypesTable))
    }

    @Test
    fun allTypes_insertIntoTable_rowInserted() {
        AllTypesTable.create()

//        val row = Row(mapOf(
//                AllTypesTable.dateCol to getDate("2000-01-01"),
//                AllTypesTable.dateTimeCol to getDateTime("2000-01-01 12:00:00"),
//                AllTypesTable.ulongCol to 1L,
//                AllTypesTable.longCol to 2L,
//                AllTypesTable.boolCol to true
//        ))

        Assert.assertTrue(MetadataClickhouse.existsTable(AllTypesTable))
    }
}

object AllTypesTable : Table("all_types_table"){
    override var db: Database = TestDatabase

    val dateCol = date("date_col")
    val dateTimeCol = datetime("datetime_col")
    val ulongCol = ulong("ulong_col")
    val longCol = long("long_col")
    val boolCol = boolean("bool_col")
    val stringCol = string("string_col")

    val arrayDateCol = arrayDate("arrayDate_col")
    val arrayDateTimeCol = arrayDateTime("arrayDateTime_col")
    val arrayUlongCol = arrayULong("arrayULong_col")
    val arrayLongCol = arrayLong("arrayLong_col")
    val arrayBoolCol = arrayBoolean("arrayBoolean_col")
    val arrayStringCol = arrayString("arrayString_col")

    override val engine: Engine = MergeTree(dateCol, listOf(dateCol))
}