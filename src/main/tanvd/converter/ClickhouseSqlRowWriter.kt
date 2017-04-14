package tanvd.converter

import ru.yandex.clickhouse.ClickHouseUtil
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.clickhouse.model.DbRow
import java.io.PrintWriter

internal class ClickhouseSqlRowWriter {

    private val types: List<String>

    val writer: PrintWriter

    constructor(filePath: String, header: List<String>) {
        writer = PrintWriter(filePath)
        types = header
    }

    fun writeHeader() {
        val sqlInsert = "INSERT INTO example.${AuditDaoClickhouseImpl.auditTable} (${AuditDaoClickhouseImpl.descriptionColumn}," +
                " ${AuditDaoClickhouseImpl.unixTimeStampColumn}, ${types.joinToString()}) VALUES"
        writer.println(sqlInsert)

    }

    fun write(row: DbRow) {
        val sqlInsert = "(${row.toValues(types)})"
        writer.println(sqlInsert)
    }

    fun flush() {
        writer.println(";")
        writer.flush()
    }

    fun close() {
        System.out.println("create table example.Audit (DateColumn Date default today()) ENGINE = MergeTree(DateColumn, (DateColumn), 8192);")
        System.out.println("ALTER TABLE example.${AuditDaoClickhouseImpl.auditTable} ADD COLUMN ${AuditDaoClickhouseImpl.descriptionColumn} Array(String);")
        System.out.println("ALTER TABLE example.${AuditDaoClickhouseImpl.auditTable} ADD COLUMN ${AuditDaoClickhouseImpl.unixTimeStampColumn} UInt64;")
        for (type in types) {
            System.out.println("ALTER TABLE example.${AuditDaoClickhouseImpl.auditTable} ADD COLUMN $type Array(String);")
        }
        writer.close()
    }

    private fun DbRow.toValues(types: List<String>): String {
        val result = StringBuilder()
        result.append(this.columns.find { it.name == AuditDaoClickhouseImpl.descriptionColumn }!!.elements.joinToString(prefix = "[", postfix = "]") { "\'$it\'" })
        result.append(", ")
        result.append(this.columns.find { it.name == AuditDaoClickhouseImpl.unixTimeStampColumn }!!.elements[0])
        result.append(", ")


        for (type in types) {
            val obj = this.columns.find { it.name == type }
            if (obj != null) {
                result.append(obj.elements.map { "\'" + ClickHouseUtil.escape(it) + "\'" }.joinToString(prefix = "[", postfix = "]"))
                result.append(", ")
            } else {
                result.append("[], ")
            }
        }

        return result.toString().dropLast(2)
    }

}
