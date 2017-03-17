package tanvd.audit.implementation.mysql

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import java.sql.Connection
import java.util.*

/**
 * Dao to SQL DB.
 * Please, remember not to use dots in names or parameters
 */
class AuditDaoMysqlImpl(rawConnection : Connection) : AuditDao {

    companion object Config {
        val types : MutableList<AuditType<Any>> = ArrayList()
    }

    val mysqlConnection: JdbcMysqlConnection
    init {

        mysqlConnection = JdbcMysqlConnection(rawConnection)

        initTables()
    }

    /**
     * Creates necessary tables for current types
     */
    private fun initTables() {
        //TODO take a closer look to length of descrpition
        val header = listOf(Pair("ID", "int"), Pair("description", "varchar(255)"))
        mysqlConnection.createTable("Audit", header, "ID", true)

        for (type in types) {
            val headerConnect = listOf(Pair("ID", "int"), Pair("TYPEID", "varchar(255)"))
            mysqlConnection.createTable(type.code, headerConnect, "ID, TYPEID", false)
        }
    }

    /**
     * Saves audit record and all its objects into appropriate tables (separate tables for objects for better search)
     */
    override fun saveRow(auditRecord: AuditRecord) {
        val stringToSave = AuditRecord.serialize(auditRecord)
        val auditId = mysqlConnection.insertRow("Audit", listOf("description"), listOf(Pair(stringToSave, String::class)))

        for ((type, id) in auditRecord.objects.toSet()) {
            if (type.klass != String::class) {
                mysqlConnection.insertRow(type.code, listOf("ID", "TYPEID"),
                        listOf(Pair(auditId.toString(), Int::class), Pair(id, String::class)))
            }
        }
    }

    /**
     * Adds new type and creates tables for it
     */
    override fun <T> addType(type : AuditType<T>) {
        val headerConnect = listOf(Pair("ID", "int"), Pair("TYPEID", "varchar(255)"))
        mysqlConnection.createTable(type.code, headerConnect, "ID, TYPEID", false)

        synchronized (types) {
            @Suppress("UNCHECKED_CAST")
            types.add(type as AuditType<Any>)
        }
    }

    /**
     * Loads all auditRecords with specified object
     */
    override fun <T>loadRow(type : AuditType<T>, id : String) : List<AuditRecord> {
        val resultList = mysqlConnection.loadRows(type.code, id)
        val auditRecordList = resultList.map {  AuditRecord.deserialize(it) }
        return auditRecordList
    }
}