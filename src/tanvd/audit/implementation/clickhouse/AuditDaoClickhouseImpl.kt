package tanvd.audit.implementation.clickhouse

import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import java.sql.Connection
import java.util.*

/**
 * Dao to Clickhouse DB.
 */
class AuditDaoClickhouseImpl(rawConnection : Connection) : AuditDao {


    companion object Config {
        val auditTableName = "Audit"
        val auditDateColumnName = "audit_date"
        val auditDescriptionColumnName = "description"
        val types : MutableSet<AuditType<Any>> = HashSet()
    }

    val clickhouseConnection: JdbcClickhouseConnection
    init {
        clickhouseConnection = JdbcClickhouseConnection(rawConnection)
        initTables()
    }

    /**
     * Creates necessary columns for current types
     */
    private fun initTables() {
        //TODO take a closer look to length of description
        val tableHeader = DbTableHeader(arrayListOf(
                DbColumnHeader(auditDateColumnName, DbColumnType.DbDate),
                DbColumnHeader(auditDescriptionColumnName, DbColumnType.DbString)))
        types.mapTo(tableHeader.columnsHeader) { DbColumnHeader(it.code, DbColumnType.DbArrayString) }
        clickhouseConnection.createTable(auditTableName, tableHeader, auditDateColumnName, auditDateColumnName)
    }

    /**
     * Saves audit record and all its objects
     */
    override fun saveRow(auditRecord: AuditRecord) {
        val (stringToSave, row) = ClickhouseRecordSerializer.serialize(auditRecord)
        row.columns.add(DbColumn(auditDescriptionColumnName, arrayListOf(stringToSave), DbColumnType.DbString))

        clickhouseConnection.insertRow(auditTableName, row)
    }

    /**
     * Saves audit records. Makes it faster, than for with saveRow
     */
    override fun saveRows(auditRecords: List<AuditRecord>) {
        val tableHeader = DbTableHeader(arrayListOf(
                DbColumnHeader(auditDescriptionColumnName, DbColumnType.DbString)))
        types.mapTo(tableHeader.columnsHeader) { DbColumnHeader(it.code, DbColumnType.DbArrayString) }

        val rows = ArrayList<DbRow>()
        for (auditRecord in auditRecords) {
            val (stringToSave, row) = ClickhouseRecordSerializer.serialize(auditRecord)
            row.columns.add(DbColumn(auditDescriptionColumnName, arrayListOf(stringToSave), DbColumnType.DbString))
            rows.add(row)
        }

        clickhouseConnection.insertRows(auditTableName, tableHeader, rows)
    }

    /**
     * Adds new type and creates column for it
     */
    override fun <T> addType(type : AuditType<T>) {
        clickhouseConnection.addColumn(auditTableName, DbColumnHeader(type.code, DbColumnType.DbArrayString))

        synchronized (types) {
            @Suppress("UNCHECKED_CAST")
            types.add(type as AuditType<Any>)
        }
    }

    /**
     * Loads all auditRecords with specified object
     */
    override fun <T>loadRow(type : AuditType<T>, id : String) : List<AuditRecord> {
        val resultList = clickhouseConnection.loadRows("Audit", type.code, id)
        val auditRecordList = resultList.map {  ClickhouseRecordSerializer.deserialize(it) }
        return auditRecordList
    }

    override fun dropTable(tableName : String) {
        clickhouseConnection.dropTable(tableName, true)
    }
}

