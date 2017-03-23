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
class AuditDaoMysqlImpl(dataSource: DataSource) : AuditDao {

    companion object Config {
        val auditTable = PropertyLoader.load("schemeMySQL.properties", "AuditTable")
        val auditIdColumn = PropertyLoader.load("schemeMySQL.properties", "AuditIdColumn")
        val descriptionColumn = PropertyLoader.load("schemeMySQL.properties", "DescriptionColumn")
        val typeIdColumn = PropertyLoader.load("schemeMySQL.properties", "TypeIdColumn")
        val auditIdInTypeTable = PropertyLoader.load("schemeMySQL.properties", "AuditIdInTypeTable")
    }

    val mysqlConnection: JdbcMysqlConnection = JdbcMysqlConnection(dataSource)

    init {
        initTables()
    }

    /**
     * Creates necessary tables for current types
     */
    private fun initTables() {
        val header = DbTableHeader(listOf(
                DbColumnHeader(auditIdColumn, DbColumnType.DbInt),
                DbColumnHeader(descriptionColumn, DbColumnType.DbString)))
        mysqlConnection.createTable(auditTable, header, auditIdColumn, true)

        for (type in AuditType.getTypes()) {
            val headerConnect = DbTableHeader(listOf(
                    DbColumnHeader(auditIdInTypeTable, DbColumnType.DbInt),
                    DbColumnHeader(typeIdColumn, DbColumnType.DbString)))
            mysqlConnection.createTable(type.code, headerConnect, "$auditIdInTypeTable, $typeIdColumn", false)
        }
    }

    /**
     * Saves audit record and all its objects into appropriate tables (separate tables for objects for better search)
     */
    override fun saveRecord(auditRecord: AuditRecord) {
        val stringToSave = MysqlRecordSerializer.serialize(auditRecord)
        val auditId = mysqlConnection.insertRow(auditTable,
                DbRow(listOf(DbColumn(descriptionColumn, stringToSave, DbColumnType.DbString))))

        for ((type, id) in auditRecord.objects.toSet()) {
            mysqlConnection.insertRow(type.code,
                    DbRow(listOf(DbColumn(auditIdInTypeTable, auditId.toString(), DbColumnType.DbInt),
                                 DbColumn(typeIdColumn, id, DbColumnType.DbString))))
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
        val header = DbTableHeader(listOf(DbColumnHeader(auditIdInTypeTable, DbColumnType.DbInt),
                                   DbColumnHeader(typeIdColumn, DbColumnType.DbString)))
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

    override fun dropTable(tableName: String) {
        mysqlConnection.dropTable(tableName, true)
    }
}