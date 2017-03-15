package tanvd.audit.implementation

import tanvd.audit.AuditRecord
import tanvd.audit.implementation.jdbc.JdbcConnection
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*
import kotlin.reflect.KClass

/**
 * Dao to SQL DB.
 * Please, remember not to use dots in names or parameters
 */
class AuditDao() {

    private fun KClass<*>.qualifiedNameForDb() : String = this.qualifiedName!!.replace('.', '_')

    companion object Config {
        val types : MutableList<KClass<*>> = ArrayList()
    }

    val rawConnection : Connection
    val connection : JdbcConnection
    init {
        try {
            rawConnection = DriverManager.getConnection("jdbc:mysql://localhost/example?useLegacyDatetimeCode=false" +
                    "&serverTimezone=Europe/Moscow", "root", "root")
        } catch (e : SQLException) {
           e.printStackTrace()
           error("Connection Failed!")
        }

        connection = JdbcConnection(rawConnection)

        initTables()
    }

    /**
     * Creates necessary tables for current types
     */
    private fun initTables() {
        //TODO take a closer look to length of descrpition
        val header = listOf(Pair("ID", "int"), Pair("description", "varchar(255)"))
        connection.createTable("Audit", header, "ID", true)

        for (type in types) {
            val qualifiedName = type.qualifiedNameForDb()
            val headerConnect = listOf(Pair("ID", "int"), Pair("TYPEID", "varchar(255)"))
            connection.createTable(qualifiedName, headerConnect, "ID, TYPEID", false)
        }
    }

    /**
     * Saves audit record and all its objects into appropriate tables (separate tables for objects for better search)
     */
    fun saveRow(auditRecord: AuditRecord) : Unit {
        val stringToSave = AuditRecord.serialize(auditRecord)
        val auditId = connection.insertRow("Audit", listOf("description"), listOf(Pair(stringToSave, String::class)))

        for ((type, id) in auditRecord.objects.toSet()) {
            if (type != String::class) {
                connection.insertRow(type.qualifiedNameForDb(), listOf("ID", "TYPEID"),
                        listOf(Pair(auditId.toString(), Int::class), Pair(id, String::class)))
            }
        }
    }

    /**
     * Adds new type and creates tables for it
     */
    fun addColumn(type : KClass<*>) {
        val qualifiedName = type.qualifiedNameForDb()
        val headerConnect = listOf(Pair("ID", "int"), Pair("TYPEID", "varchar(255)"))
        connection.createTable(qualifiedName, headerConnect, "ID, TYPEID", false)

        synchronized (types) {
            types.add(type)
        }
    }

    /**
     * Loads all auditRecords with specified object
     */
    fun loadRow(type : KClass<*>, id : String) : List<AuditRecord> {
        val resultList = connection.loadRows(type.qualifiedNameForDb(), id)
        val auditRecordList = resultList.map {  AuditRecord.deserialize(it, types) }
        return auditRecordList
    }
}