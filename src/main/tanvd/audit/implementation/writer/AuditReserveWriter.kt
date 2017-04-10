package tanvd.audit.implementation.writer

import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.PropertyLoader

internal interface AuditReserveWriter {

    fun write(record: AuditRecordInternal)

    fun flush()

    companion object AuditReserveWriterFactory {


        private val filePath: String = PropertyLoader.loadProperty("ReserveFilePath") ?: "reserve.txt"

        private var internalWriter: AuditReserveWriter = ClickhouseSqlFileWriter(filePath)

        fun getWriter(): AuditReserveWriter {
            return internalWriter
        }
    }
}
