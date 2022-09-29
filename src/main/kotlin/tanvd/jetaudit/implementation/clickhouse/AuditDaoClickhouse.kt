package tanvd.jetaudit.implementation.clickhouse

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.count
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.query.*
import tanvd.jetaudit.implementation.clickhouse.aorm.AuditTable
import tanvd.jetaudit.implementation.clickhouse.aorm.withAuditDatabase
import tanvd.jetaudit.model.external.presenters.IsDeletedType
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.internal.AuditRecordInternal

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

    override fun <T : Any> addTypeInDbModel(type: ObjectType<T>): Unit = withAuditDatabase {
        for (stateType in type.state) {
            if (AuditTable.useDDL) {
                @Suppress("UNCHECKED_CAST")
                AuditTable.addColumn(stateType.column as Column<List<T>, DbType<List<T>>>)
            } else {
                AuditTable.columns.add(stateType.column)
            }
        }
    }

    override fun <T : Any> addInformationInDbModel(information: InformationType<T>): Unit = withAuditDatabase {
        if (AuditTable.useDDL) {
            AuditTable.addColumn(information.column)
        } else {
            AuditTable.columns.add(information.column)
        }
    }

    override fun loadRecords(expression: QueryExpression, orderByExpression: OrderByExpression?,
                             limitExpression: LimitExpression?) = withAuditDatabase {
        // performance optimization via two queries: 1st - preselect primary keys, 2nd - load records
        // see: https://github.com/ClickHouse/ClickHouse/issues/7187#issuecomment-538174102
        // and: https://github.com/ClickHouse/ClickHouse/issues/7187#issuecomment-539047920
        val orderBySql = orderByExpression?.let {
            " ORDER BY ${orderByExpression.map.entries.joinToString { "${it.key.toQueryQualifier()} ${it.value}" }}"
        }.orEmpty()

        val pkColumns = arrayOf(AuditTable.date, AuditTable.id, AuditTable.timestamp)

        val idsQuery = AuditTable.select(*pkColumns).prewhere(expression).apply {
            orderByExpression?.let { orderBy(it) }
            limitExpression?.let { limit(it) }
        }

        val ids = QueryClickhouse.getResult(db, idsQuery).map {
            listOf(it[AuditTable.date], it[AuditTable.id], it[AuditTable.timestamp])
        }.ifEmpty { return@withAuditDatabase emptyList() }

        val sqlQuery = buildString {
            append("SELECT ")
            AuditTable.columns.joinTo(this) {
                it.toSelectListDef()
            }
            append(" FROM ${AuditTable.name}")
            append(" PREWHERE (")
            pkColumns.joinTo(this) { it.toSelectListDef() }
            append(") IN (")
            ids.joinTo(this) {
                "(?, ?, ?)"
            }
            append(")")
            append(orderBySql)
        }
        @Suppress("UNCHECKED_CAST") val params = ids.flatMap {
            pkColumns.mapIndexed { indx, col ->
                col.type to it[indx]
            }
        } as List<Pair<DbType<Any>, Any>>
        val rows = QueryClickhouse.getResult(db, PreparedSqlResult(sqlQuery, params), AuditTable.columns)
        val deletedMarker = InformationObject(true, IsDeletedType)
        //filter to newest version
        rows.groupBy { row ->
            row[AuditTable.id]
        }.mapNotNull { (_, rows) ->
            val row = rows.maxBy { row -> row[AuditTable.version] }
            ClickhouseRecordSerializer.deserialize(row).takeIf { r ->
                deletedMarker !in r.information
            }
        }
    }

    override fun countRecords(expression: QueryExpression) = withAuditDatabase {
        val countExpression = count(AuditTable.id)
        val query = AuditTable.select(countExpression) prewhere expression

        val resultList = query.toResult()
        resultList.singleOrNull()?.let {
            it[countExpression]
        } ?: 0L
    }
}