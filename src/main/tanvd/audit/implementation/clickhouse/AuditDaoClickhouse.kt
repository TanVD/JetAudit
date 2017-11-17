package tanvd.audit.implementation.clickhouse

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.count
import tanvd.aorm.query.*
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal

open internal class AuditDaoClickhouse : AuditDao {

    override fun initTable() = ddlRequest {
        AuditTable().create()
    }

    override fun saveRecord(auditRecordInternal: AuditRecordInternal) {
        AuditTable().insert {
            it.putAll(ClickhouseRecordSerializer.serialize(auditRecordInternal))
        }
    }

    override fun saveRecords(auditRecordInternals: List<AuditRecordInternal>) {
        AuditTable().batchInsert(auditRecordInternals, AuditTable().columns.toList()) { row, value ->
            row.putAll(ClickhouseRecordSerializer.serialize(value))
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> addTypeInDbModel(type: ObjectType<T>) = ddlRequest {
        for (stateType in type.state) {
            AuditTable().addColumn(stateType.column as Column<List<T>, DbType<List<T>>>)
        }
    }

    override fun <T : Any> addInformationInDbModel(information: InformationType<T>) = ddlRequest {
        AuditTable().addColumn(information.column)
    }

    override fun loadRecords(expression: QueryExpression, orderByExpression: OrderByExpression?,
                             limitExpression: LimitExpression?): List<AuditRecordInternal> {
        var query = AuditTable().select() prewhere expression

        if (AuditTable().useIsDeleted) {
            query.prewhereSection = query.prewhereSection!! and (AuditTable().isDeleted eq false)
        }
        if (limitExpression != null) {
            query = query limit limitExpression
        }
        if (orderByExpression != null) {
            query = query orderBy orderByExpression
        }

        val rows = query.toResult()
        //filter to newest version
        val rowsFiltered = rows.groupBy { row ->
            row[AuditTable().id] as Long
        }.mapValues {
            it.value.sortedByDescending { row ->
                row[AuditTable().version]!!.toLong()
            }.first()
        }.values.toList()

        return rowsFiltered.map { ClickhouseRecordSerializer.deserialize(it) }
    }

    override fun countRecords(expression: QueryExpression): Long {
        val query = AuditTable().select(count(AuditTable().id)) prewhere expression
        if (AuditTable().useIsDeleted) {
            query.prewhereSection = query.prewhereSection!! and (AuditTable().isDeleted eq false)
        }

        val resultList = query.toResult()
        return resultList.singleOrNull()?.let {
            it.values.values.single() as Long
        } ?: 0L
    }

    private fun ddlRequest(body: () -> Unit) {
        if (AuditTable().useDDL) {
            body()
        }
    }
}