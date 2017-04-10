package tanvd.audit.implementation.writer

import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.model.internal.AuditRecordInternal
import java.io.PrintWriter

internal class ClickhouseSqlFileWriter(filePath: String) : AuditReserveWriter {

    val writer = PrintWriter(filePath)

    override fun write(record: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(record)
        val sqlInsert = "INSERT INTO ${AuditDaoClickhouseImpl.auditTable} (${row.toStringHeader()})" +
                " VALUES (${row.toValues()});"
        writer.println(sqlInsert)
    }

    override fun flush() {
        writer.flush()
    }

    private fun DbRow.toValues(): String {
        return columns.map { (_, elements) ->
            elements.map { "\'" + it + "\'" }.
                    joinToString(prefix = "[", postfix = "]")
        }.joinToString()
    }

}
