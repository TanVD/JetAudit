package tanvd.audit.implementation.mysql

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import javax.sql.DataSource

/**
 * Dao to MySQL DB.
 * Please, remember not to use dots in names or parameters
 */
class AuditDaoMysqlImpl(dataSource: DataSource) : AuditDao {

    val mysqlConnection: JdbcMysqlConnection = JdbcMysqlConnection(dataSource)

    init {
        initTables()
    }

    /**
     * Creates necessary tables for current types
     */
    private fun initTables() {
        //TODO take a closer look to length of descrpition
        val header = listOf(Pair("ID", "int"), Pair("description", "varchar(255)"))
        mysqlConnection.createTable("Audit", header, "ID", true)

        for (type in AuditType.getTypes()) {
            val headerConnect = listOf(Pair("ID", "int"), Pair("TYPEID", "varchar(255)"))
            mysqlConnection.createTable(type.code, headerConnect, "ID, TYPEID", false)
        }
    }

    /**
     * Saves audit record and all its objects into appropriate tables (separate tables for objects for better search)
     */
    override fun saveRecord(auditRecord: AuditRecord) {
        val stringToSave = MysqlRecordSerializer.serialize(auditRecord)
        val auditId = mysqlConnection.insertRow("Audit", listOf("description"), listOf(Pair(stringToSave, String::class)))

        for ((type, id) in auditRecord.objects.toSet()) {
            mysqlConnection.insertRow(type.code, listOf("ID", "TYPEID"),
                    listOf(Pair(auditId.toString(), Int::class), Pair(id, String::class)))
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
        val headerConnect = listOf(Pair("ID", "int"), Pair("TYPEID", "varchar(255)"))
        mysqlConnection.createTable(type.code, headerConnect, "ID, TYPEID", false)
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