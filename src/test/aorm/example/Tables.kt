//package aorm.example
//
//import org.joda.time.DateTime
//import tanvd.aorm.Database
//import tanvd.aorm.MergeTree
//import tanvd.aorm.Table
//import tanvd.aorm.default
//import utils.TestDatabase
//import java.util.*
//
//object AuditTable : Table("Audit") {
//    override val db: Database = TestDatabase
//
//    val date = date("date_column").default { DateTime.now().toDate() }
//    val id = long("id_column").default { Random().nextLong() }
//    val version = ulong("version_column")
//
//    override val engine = MergeTree(date, listOf(date, id))
//}
