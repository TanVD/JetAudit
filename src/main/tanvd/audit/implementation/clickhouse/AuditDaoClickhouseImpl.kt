package tanvd.audit.implementation.clickhouse

import tanvd.audit.implementation.clickhouse.model.DbColumnHeader
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbTableHeader
import tanvd.audit.implementation.clickhouse.model.toDbColumnHeader
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.queries.QueryExpression
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.PropertyLoader
import javax.sql.DataSource

/**
 * Dao to Clickhouse DB.
 */
internal class AuditDaoClickhouseImpl(dataSource: DataSource) : AuditDao {


    /**
     * Predefined scheme for clickhouse base.
     */
    companion object Scheme {
        val auditTable = PropertyLoader.loadProperty("AuditTable") ?: "Audit"

        val dateColumn = PropertyLoader.loadProperty("DateColumn") ?: "Audit_Date"
        val descriptionColumn = PropertyLoader.loadProperty("DescriptionColumn") ?: "Description"

        /** Default column with date for MergeTree Engine. Should not be inserted or selected.*/
        val defaultColumns = arrayOf(DbColumnHeader(dateColumn, DbColumnType.DbDate))

        /**
         * Mandatory columns for audit. Should be presented in every insert. Treated specifically rather than
         * normal types.
         */
        val mandatoryColumns = arrayOf(DbColumnHeader(descriptionColumn, DbColumnType.DbArrayString))

        fun getInformationColumns(): Array<DbColumnHeader> {
            return InformationType.getTypes().
                    map { DbColumnHeader(it.code, DbColumnType.getFromInformationInnerType(it.type)) }.toTypedArray()
        }

        fun getPredefinedAuditTableColumn(name: String): DbColumnHeader {
            return arrayOf(*defaultColumns, *mandatoryColumns).find { it.name == name }!!
        }

    }

    private val clickhouseConnection = JdbcClickhouseConnection(dataSource)

    init {
        initTables()
    }

    /**
     * Creates necessary tables to start
     *
     * @throws BasicDbException
     */
    private fun initTables() {
        val columnsHeader = arrayListOf(*defaultColumns, *mandatoryColumns, *getInformationColumns())
        AuditType.getTypes().mapTo(columnsHeader) { DbColumnHeader(it.code, DbColumnType.DbArrayString) }
        clickhouseConnection.createTable(auditTable, DbTableHeader(columnsHeader),
                listOf(dateColumn, InformationType.resolveType(IdPresenter).code,
                        InformationType.resolveType(TimeStampPresenter).code), dateColumn,
                InformationType.resolveType(VersionPresenter).code)
    }

    /**
     * Saves audit record and all its objects
     *
     * @throws BasicDbException
     */
    override fun saveRecord(auditRecordInternal: AuditRecordInternal) {
        val row = ClickhouseRecordSerializer.serialize(auditRecordInternal)
        clickhouseConnection.insertRow(auditTable, row)
    }

    /**
     * Saves audit records. Makes it faster, than for loop with saveRecord
     *
     * @throws BasicDbException
     */
    override fun saveRecords(auditRecordInternals: List<AuditRecordInternal>) {
        val columnsHeader = arrayListOf(*mandatoryColumns, *getInformationColumns())
        AuditType.getTypes().mapTo(columnsHeader) { DbColumnHeader(it.code, DbColumnType.DbArrayString) }

        val rows = auditRecordInternals.map { ClickhouseRecordSerializer.serialize(it) }

        clickhouseConnection.insertRows(auditTable, DbTableHeader(columnsHeader), rows)
    }

    /**
     * Adds new type and creates column for it
     *
     * @throws BasicDbException
     */
    override fun <T> addTypeInDbModel(type: AuditType<T>) {
        clickhouseConnection.addColumn(auditTable, DbColumnHeader(type.code, DbColumnType.DbArrayString))

    }

    override fun <T> addInformationInDbModel(information: InformationType<T>) {
        clickhouseConnection.addColumn(auditTable, information.toDbColumnHeader())
    }

    /**
     * Loads all auditRecords with specified object
     *
     * @throws BasicDbException
     */
    override fun loadRecords(expression: QueryExpression, parameters: QueryParameters): List<AuditRecordInternal> {
        val selectColumns = arrayListOf(*mandatoryColumns, *getInformationColumns())
        AuditType.getTypes().mapTo(selectColumns) { DbColumnHeader(it.code, DbColumnType.DbArrayString) }

        val resultList = clickhouseConnection.loadRows(auditTable, DbTableHeader(selectColumns), expression, parameters)
        val auditRecordList = resultList.map { ClickhouseRecordSerializer.deserialize(it) }
        return auditRecordList
    }

    /**
     * Return total count of records satisfying condition
     *
     * @throws BasicDbException
     */
    override fun countRecords(expression: QueryExpression): Int {
        val resultNumber = clickhouseConnection.countRows(auditTable, expression)
        return resultNumber
    }

    /**
     * Drops table with specified name
     *
     * @throws BasicDbException
     */
    fun dropTable(tableName: String) {
        clickhouseConnection.dropTable(tableName, true)
    }
}

