//package aorm.example
//
//import org.testng.annotations.Test
//import tanvd.aorm.Column
//import tanvd.aorm.DbDate
//
//class AormTableCreateExample {
//    @Test
//    fun example_create() {
//        AuditTable.create()
//    }
//
//    @Test
//    fun example_drop() {
//        AuditTable.drop()
//    }
//
//    @Test
//    fun example_alter_column() {
//        AuditTable.create()
//        AuditTable.addColumn(Column("date_2_column", DbDate()))
//    }
//
//    @Test
//    fun example_drop_column() {
//        AuditTable.create()
//        AuditTable.dropColumn(AuditTable.version)
//    }
//}