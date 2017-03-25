package tanvd.audit.implementation.mysql

import org.slf4j.LoggerFactory
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.getPredefinedAuditTableColumn
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.unixTimeStampColumn
import tanvd.audit.implementation.mysql.model.DbColumn
import tanvd.audit.implementation.mysql.model.DbRow
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import java.util.*

object MysqlRecordSerializer {

    private val logger = LoggerFactory.getLogger(MysqlRecordSerializer::class.java)


    val delimiter = '\u0001'

    /**
     * Serialize MySQL.AuditRecord to String representation.
     * DbString serialized as is, types serialized as
     * [delimiter]name fully qualified name [delimiter] string id representation [delimiter]
     */
    fun serialize(auditRecord: AuditRecord): DbRow {
        val description = StringBuilder()
        for (o in auditRecord.objects) {
            description.append(delimiter)
            description.append(o.first.code)
            description.append(delimiter)
            description.append(o.second)
            description.append(delimiter)
        }
        val row = DbRow(
                DbColumn(getPredefinedAuditTableColumn(descriptionColumn), description.toString()),
                DbColumn(getPredefinedAuditTableColumn(unixTimeStampColumn), auditRecord.unixTimeStamp.toString()))
        return row
    }

    /**
     * Deserialize MySQL.AuditRecord from string representation
     */
    fun deserialize(row: DbRow): AuditRecord {
        val description = row.columns.find { it.name == descriptionColumn }
        if (description == null) {
            logger.error("MySQL scheme violated. Not found $descriptionColumn column.")
            return AuditRecord(emptyList(), 0)
        }
        val unixTimeStamp = row.columns.find { it.name == unixTimeStampColumn }
        if (unixTimeStamp == null) {
            logger.error("MySQL scheme violated. Not found $unixTimeStampColumn column.")
            return AuditRecord(emptyList(), 0)
        }
        var auditSerializedString = description.element
        val objects = ArrayList<Pair<AuditType<Any>, String>>()
        while (auditSerializedString.isNotEmpty()) {
            val code = auditSerializedString.subSequence(1, auditSerializedString.indexOf(delimiter, 1))
            auditSerializedString = auditSerializedString.drop(auditSerializedString.indexOf(delimiter, 1))
            val id = auditSerializedString.subSequence(1, auditSerializedString.indexOf(delimiter, 1)).toString()
            auditSerializedString = auditSerializedString.drop(auditSerializedString.indexOf(delimiter, 1) + 1)
            val type: AuditType<Any>? = AuditType.resolveType(code.toString())
            if (type != null) {
                objects.add(Pair(type, id))
            }
        }

        return AuditRecord(objects, unixTimeStamp.element.toInt())
    }
}