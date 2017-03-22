package tanvd.audit.implementation.clickhouse

import tanvd.audit.implementation.clickhouse.model.DbColumnHeader
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbTableHeader
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import javax.sql.DataSource

/**
 * Dao to Clickhouse DB.
 */
class AuditDaoClickhouseImpl(dataSource: DataSource) : AuditDao {

    companion object Config {
        const val auditTableName = "Audit"
        const val auditDateColumnName = "audit_date"
        const val auditDescriptionColumnName = "description"
    }

    private val clickhouseConnection  = JdbcClickhouseConnection(dataSource)

    init {
        initTables()
    }

    /**
     * Creates necessary columns for current types
     */
    private fun initTables() {
        val tableHeader = DbTableHeader(arrayListOf(
                DbColumnHeader(auditDateColumnName, DbColumnType.DbDate),
                DbColumnHeader(auditDescriptionColumnName, DbColumnType.DbArrayString)))
        AuditType.getTypes().mapTo(tableHeader.columnsHeader) { DbColumnHeader(it.code, DbColumnType.DbArrayString) }
        clickhouseConnection.createTable(auditTableName, tableHeader, auditDateColumnName, auditDateColumnName)
    }

    /**
     * Saves audit record and all its objects
     */
    override fun saveRecord(auditRecord: AuditRecord) {
        val row = ClickhouseRecordSerializer.serialize(auditRecord)
        clickhouseConnection.insertRow(auditTableName, row)
    }

    /**
     * Saves audit records. Makes it faster, than for loop with saveRecord
     */
    override fun saveRecords(auditRecords: List<AuditRecord>) {
        val tableHeader = DbTableHeader(arrayListOf(
                DbColumnHeader(auditDescriptionColumnName, DbColumnType.DbArrayString)))
        AuditType.getTypes().mapTo(tableHeader.columnsHeader) { DbColumnHeader(it.code, DbColumnType.DbArrayString) }

        val rows = auditRecords.map { ClickhouseRecordSerializer.serialize(it) }

        clickhouseConnection.insertRows(auditTableName, tableHeader, rows)
    }

    /**
     * Adds new type and creates column for it
     */
    override fun <T> addTypeInDbModel(type : AuditType<T>) {
        clickhouseConnection.addColumn(auditTableName, DbColumnHeader(type.code, DbColumnType.DbArrayString))
    }

    /**
     * Loads all auditRecords with specified object
     */
    override fun <T> loadRecords(type : AuditType<T>, id : String) : List<AuditRecord> {
        val resultList = clickhouseConnection.loadRows("Audit", type.code, id)
        val auditRecordList = resultList.map {  ClickhouseRecordSerializer.deserialize(it) }
        return auditRecordList
    }

    /**
     * Drops table with specified name
     */
    override fun dropTable(tableName : String) {
        clickhouseConnection.dropTable(tableName, true)
    }
}

