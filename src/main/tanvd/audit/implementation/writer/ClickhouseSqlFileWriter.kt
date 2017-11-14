package tanvd.audit.implementation.writer

import tanvd.aorm.Column
import tanvd.aorm.DbType
import tanvd.aorm.InsertExpression
import tanvd.aorm.Row
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

    @Suppress("UNCHECKED_CAST")
    override fun write(record: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(record)
        writer.println(InsertExpression(AuditTable, Row(row as Map<Column<Any, DbType<Any>>, Any>)).toSql())
    }

    override fun flush() {
        writer.flush()
    }

    override fun close() {
        writer.close()
    }
}
