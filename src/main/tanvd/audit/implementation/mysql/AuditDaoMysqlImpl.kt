package tanvd.audit.implementation.mysql

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.mysql.model.*
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import tanvd.audit.utils.PropertyLoader
import javax.sql.DataSource

/**
 * Dao to MySQL DB.
 * Please, remember not to use dots in names or parameters
 */
internal class AuditDaoMysqlImpl(dataSource: DataSource) : AuditDao {

    /**
     * Predefined scheme for MySQL base.
     */
    companion object Scheme {
        val auditTable = PropertyLoader.load("schemeMySQL.properties", "AuditTable")
        val auditIdColumn = PropertyLoader.load("schemeMySQL.properties", "AuditIdColumn")
        val descriptionColumn = PropertyLoader.load("schemeMySQL.properties", "DescriptionColumn")
        val unixTimeStampColumn = PropertyLoader.load("schemeMySQL.properties", "UnixTimeStampColumn")
        val typeIdColumn = PropertyLoader.load("schemeMySQL.properties", "TypeIdColumn")
        val auditIdInTypeTable = PropertyLoader.load("schemeMySQL.properties", "AuditIdInTypeTable")

        /** Default column with date for MergeTree Engine. Should not be inserted or selected.*/
        val defaultColumnsAuditTable = arrayOf(DbColumnHeader(auditIdColumn, DbColumnType.DbInt))

        /**
         * Mandatory columns for audit. Should be presented in every insert. Treated specifically rather than
         * normal types.
         */
        val mandatoryColumnsAuditTable = arrayOf(
                DbColumnHeader(descriptionColumn, DbColumnType.DbString),
                DbColumnHeader(unixTimeStampColumn, DbColumnType.DbInt))

        /** Mandatory columns for types tables. Should be presented in every insert to types table.**/
        val mandatoryColumnsTypeTable = arrayOf(
                DbColumnHeader(auditIdInTypeTable, DbColumnType.DbInt),
                DbColumnHeader(typeIdColumn, DbColumnType.DbString))

        fun getPredefinedAuditTableColumn(name: String): DbColumnHeader {
            return arrayOf(*defaultColumnsAuditTable, *mandatoryColumnsAuditTable).find { it.name == name }!!
        }

        fun getPredefinedTypeTableColumn(name: String): DbColumnHeader {
            return arrayOf(*mandatoryColumnsTypeTable).find { it.name == name }!!
        }
    }

    val mysqlConnection: JdbcMysqlConnection = JdbcMysqlConnection(dataSource)

    init {
        initTables()
    }

    /**
     * Creates necessary tables for current types
     */
    private fun initTables() {
        val header = DbTableHeader(*defaultColumnsAuditTable, *mandatoryColumnsAuditTable)
        mysqlConnection.createTable(auditTable, header, auditIdColumn, true)

        for (type in AuditType.getTypes()) {
            val headerConnect = DbTableHeader(*mandatoryColumnsTypeTable)
            mysqlConnection.createTable(type.code, headerConnect, "$auditIdInTypeTable, $typeIdColumn", false)
        }
    }

    /**
     * Saves audit record and all its objects into appropriate tables (separate tables for objects for better search)
     */
    override fun saveRecord(auditRecord: AuditRecord) {
        val row = MysqlRecordSerializer.serialize(auditRecord)
        val auditId = mysqlConnection.insertRow(auditTable, row)

        for ((type, typeId) in auditRecord.objects.toSet()) {
            mysqlConnection.insertRow(type.code,
                    DbRow(DbColumn(getPredefinedTypeTableColumn(auditIdInTypeTable), auditId.toString()),
                            DbColumn(getPredefinedTypeTableColumn(typeIdColumn), typeId)))
        }
    }

    override fun saveRecords(auditRecords: List<AuditRecord>) {
        for (auditRecord in auditRecords) {
            saveRecord(auditRecord)
        }
    }

    /**
     * Adds new name and creates tables for it
     */
    override fun <T> addTypeInDbModel(type: AuditType<T>) {
        val header = DbTableHeader(*mandatoryColumnsTypeTable)
        mysqlConnection.createTable(type.code, header, "$auditIdInTypeTable, $typeIdColumn", false)
    }

    /**
     * Loads all auditRecords with specified object
     */
    override fun <T> loadRecords(type: AuditType<T>, id: String): List<AuditRecord> {
        val resultList = mysqlConnection.loadRows(type.code, id)
        val auditRecordList = resultList.map { MysqlRecordSerializer.deserialize(it) }
        return auditRecordList
    }

    fun dropTable(tableName: String) {
        mysqlConnection.dropTable(tableName, true)
    }
}