package tanvd.jetaudit.implementation.clickhouse

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.count
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.query.*
import tanvd.jetaudit.implementation.clickhouse.aorm.AuditTable
import tanvd.jetaudit.implementation.clickhouse.aorm.withAuditDatabase
import tanvd.jetaudit.model.external.presenters.IsDeletedType
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
        // performance optimization via sub-query to preselect primary keys
        // see: https://github.com/ClickHouse/ClickHouse/issues/7187#issuecomment-538174102
        // and: https://github.com/ClickHouse/ClickHouse/issues/7187#issuecomment-539047920
        val (exprSql, exprData) = expression.toSqlPreparedDef()
        val sqlQuery = buildString {
            val combinedKey = "${AuditTable.id.toSelectListDef()}+${AuditTable.timestamp.toSelectListDef()}"

            val orderBySql = orderByExpression?.let {
                " ORDER BY ${orderByExpression.map.toList().joinToString { "${it.first.toQueryQualifier()} ${it.second}" }}"
            }.orEmpty()

            append("SELECT ")
            AuditTable.columns.joinTo(this) {
                it.toSelectListDef()
            }
            append(" FROM ${AuditTable.name}")
            append(" PREWHERE $combinedKey IN (")
            run {
                append("SELECT $combinedKey")
                append(" FROM ${AuditTable.name}")
                append(" PREWHERE $exprSql")
                append(orderBySql)
                if (limitExpression != null) {
                    append(" LIMIT ${limitExpression.offset}, ${limitExpression.limit}")
                }
            }
            append(")")
            append(orderBySql)
        }
        val rows = QueryClickhouse.getResult(db, PreparedSqlResult(sqlQuery, exprData), AuditTable.columns)

        //filter to newest version
        val rowsFiltered = rows.groupBy { row ->
            row[AuditTable.id]
        }.mapValues {
            it.value.maxByOrNull { row ->
                row[AuditTable.version].toLong()
            }!!
        }.values

        rowsFiltered.map { ClickhouseRecordSerializer.deserialize(it) }.filterNot { r ->
            r.information.any { it.type == IsDeletedType && it.value as Boolean }
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