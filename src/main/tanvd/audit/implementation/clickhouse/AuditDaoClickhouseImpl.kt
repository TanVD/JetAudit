package tanvd.audit.implementation.clickhouse

import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.model.external.presenters.DatePresenter
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.queries.QueryExpression
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
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
        val auditTable by lazy { PropertyLoader["AuditTable"] ?: "AuditTable" }

        val descriptionColumn by lazy { PropertyLoader["DescriptionColumn"] ?: "Description" }

        /**
         * Mandatory columns for audit. Should be presented in every insert. Treated specifically rather than
         * normal types.
         */
        val mandatoryColumns = arrayOf(DbColumnHeader(descriptionColumn, DbColumnType.DbArrayString))

        fun getInformationColumns(): Array<DbColumnHeader> {
            return InformationType.getTypes().
                    map { DbColumnHeader(it.code, it.toDbColumnType()) }.toTypedArray()
        }

        fun getTypesColumns(): Array<DbColumnHeader> {
            return ObjectType.getTypes().flatMap { it.state.map { DbColumnHeader(it.getCode(), it.toDbColumnType()) } }.
                    toTypedArray()
        }

        fun getPredefinedAuditTableColumn(name: String): DbColumnHeader {
            return arrayOf(*mandatoryColumns).find { it.name == name }!!
        }

    }

    private val clickhouseConnection = JdbcClickhouseConnection(dataSource)

    private val useDefaultDDL by lazy { PropertyLoader["UseDefaultDDL"]?.toBoolean() ?: true }

    init {
        initTables()
    }

    /**
     * Creates necessary tables to start
     *
     * @throws BasicDbException
     */
    private fun initTables() {
        if (useDefaultDDL) {
            val columnsHeader = arrayListOf(*mandatoryColumns, *getInformationColumns(), *getTypesColumns())
            clickhouseConnection.createTable(auditTable, DbTableHeader(columnsHeader),
                    listOf(InformationType.resolveType(DatePresenter).code,
                            InformationType.resolveType(TimeStampPresenter).code,
                            InformationType.resolveType(IdPresenter).code),
                    InformationType.resolveType(DatePresenter).code,
                    InformationType.resolveType(VersionPresenter).code)
        }
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
     * Saves audit records. Makes it faster, than FOR loop with saveRecord
     *
     * @throws BasicDbException
     */
    override fun saveRecords(auditRecordInternals: List<AuditRecordInternal>) {
        val columnsHeader = arrayListOf(*mandatoryColumns, *getInformationColumns(), *getTypesColumns())

        val rows = auditRecordInternals.map { ClickhouseRecordSerializer.serialize(it) }

        clickhouseConnection.insertRows(auditTable, DbTableHeader(columnsHeader), rows)
    }

    /**
     * Adds new type and creates column for it
     *
     * @throws BasicDbException
     */
    override fun <T : Any> addTypeInDbModel(type: ObjectType<T>) {
        if (useDefaultDDL) {
            for (stateType in type.state) {
                clickhouseConnection.addColumn(auditTable, DbColumnHeader(stateType.getCode(), stateType.toDbColumnType()))
            }
        }
    }


    override fun <T> addInformationInDbModel(information: InformationType<T>) {
        if (useDefaultDDL) {
            clickhouseConnection.addColumn(auditTable, information.toDbColumnHeader())
        }
    }

    /**
     * Loads all auditRecords with specified object
     *
     * @throws BasicDbException
     */
    override fun loadRecords(expression: QueryExpression, parameters: QueryParameters): List<AuditRecordInternal> {
        val selectColumns = arrayListOf(*mandatoryColumns, *getInformationColumns(), *getTypesColumns())

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

