package tanvd.audit.implementation.writer

import tanvd.aorm.InsertExpression
import tanvd.aorm.InsertRow
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.internal.AuditRecordInternal
import java.io.PrintWriter

internal class ClickhouseSqlFileWriter : AuditReserveWriter {

    private val writer: PrintWriter

    constructor(filePath: String) {
        writer = PrintWriter(filePath)
    }

    constructor(writer: PrintWriter) {
        this.writer = writer
    }

    override fun write(record: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(record)
        writer.println(InsertExpression(AuditTable, InsertRow(row.toMutableMap())).toSql())
    }

    override fun flush() {
        writer.flush()
    }

    override fun close() {
        writer.close()
    }
}
