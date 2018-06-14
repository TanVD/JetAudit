package tanvd.audit.implementation.clickhouse

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.count
import tanvd.aorm.query.*
import tanvd.audit.implementation.clickhouse.aorm.AuditTable
import tanvd.audit.implementation.clickhouse.aorm.withAuditDatabase
import tanvd.audit.model.external.presenters.IsDeletedType
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal

internal open class AuditDaoClickhouse : AuditDao {

    override fun initTable() = withAuditDatabase {
        if (AuditTable.useDDL) {
            AuditTable.syncScheme()
        }
    }

    override fun saveRecord(auditRecordInternal: AuditRecordInternal) = withAuditDatabase {
        AuditTable.insert { row ->
            ClickhouseRecordSerializer.serialize(auditRecordInternal).forEach {
                row[it.key] = it.value
            }
        }
    }

    override fun saveRecords(auditRecordInternals: List<AuditRecordInternal>) = withAuditDatabase {
        AuditTable.batchInsert(auditRecordInternals, AuditTable.columns) { row, value ->
            ClickhouseRecordSerializer.serialize(value).forEach {
                row[it.key] = it.value
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> addTypeInDbModel(type: ObjectType<T>): Unit = withAuditDatabase {
        for (stateType in type.state) {
            if (AuditTable.useDDL) {
                AuditTable.addColumn(stateType.column as Column<List<T>, DbType<List<T>>>)
            } else {
                AuditTable.columns.add(stateType.column as Column<Any, DbType<Any>>)
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> addInformationInDbModel(information: InformationType<T>): Unit = withAuditDatabase {
        if (AuditTable.useDDL) {
            AuditTable.addColumn(information.column)
        } else {
            AuditTable.columns.add(information.column as Column<Any, DbType<Any>>)
        }
    }

    override fun loadRecords(expression: QueryExpression, orderByExpression: OrderByExpression?,
                             limitExpression: LimitExpression?) = withAuditDatabase {
        var query = AuditTable.select() prewhere expression

        if (limitExpression != null) {
            query = query limit limitExpression
        }
        if (orderByExpression != null) {
            query = query orderBy orderByExpression
        }

        val rows = query.toResult()
        //filter to newest version
        val rowsFiltered = rows.groupBy { row ->
            row[AuditTable.id] as Long
        }.mapValues {
            it.value.maxBy { row ->
                row[AuditTable.version]!!.toLong()
            }!!
        }.values

        rowsFiltered.map { ClickhouseRecordSerializer.deserialize(it) }.filterNot {
            it.information.any { it.type == IsDeletedType && it.value as Boolean }
        }
    }

    override fun countRecords(expression: QueryExpression) = withAuditDatabase {
        val countExpression = count(AuditTable.id)
        val query = AuditTable.select(countExpression) prewhere expression

        val resultList = query.toResult()
        resultList.singleOrNull()?.let {
            it[countExpression] as Long
        } ?: 0L
    }
}