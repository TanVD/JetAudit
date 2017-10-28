package aorm.example

import org.testng.annotations.Test

class AormTableCreateExample {
    @Test
    fun example_create() {
        AuditTable.create()
    }

    @Test
    fun example_drop() {
        AuditTable.drop()
    }

    @Test
    fun example_alter_column() {
        AuditTable.create()
        AuditTable.addColumn(DateColumn("date_2_column"))
    }

    @Test
    fun example_drop_column() {
        AuditTable.create()
        AuditTable.dropColumn(AuditTable.version)
    }
}