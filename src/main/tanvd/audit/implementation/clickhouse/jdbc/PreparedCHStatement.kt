package tanvd.audit.implementation.clickhouse.jdbc

import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.types.InnerType
import java.sql.Connection
import java.sql.PreparedStatement

internal class PreparedCHStatement(val connection: Connection,
                                   val commandSection: String,
                                   val fromSection: String,
                                   val prewhereSection: QueryExpression? = null,
                                   val whereSection: QueryExpression? = null,
                                   val parametersSection: QueryParameters = QueryParameters()) {
    private val logger = LoggerFactory.getLogger(PreparedCHStatement::class.java)

    fun prepare() : PreparedStatement {
        val sqlSelect = StringBuilder()
        sqlSelect.append("$commandSection $fromSection ")

        if (prewhereSection != null) {
            sqlSelect.append(" PREWHERE ")
            addExpressionPlaceholders(prewhereSection, sqlSelect)
        }

        if (whereSection != null) {
            sqlSelect.append(" WHERE ")
            addExpressionPlaceholders(whereSection, sqlSelect)
        }

        addOrderBy(parametersSection.orderBy, sqlSelect)

        addLimitPlaceholders(parametersSection.limits, sqlSelect)

        sqlSelect.append(";")

        val preparedStatement = connection.prepareStatement(sqlSelect.toString())


        var dbIndex = 1

        if (prewhereSection != null) {
            dbIndex = addExpressionValues(prewhereSection, preparedStatement, dbIndex)
        }

        if (whereSection != null) {
            addExpressionValues(whereSection, preparedStatement, dbIndex)
        }

        setLimitValues(parametersSection.limits, preparedStatement, dbIndex)


        return preparedStatement
    }

    private fun addExpressionPlaceholders(expression: QueryExpression, sqlSelect: StringBuilder) {
        sqlSelect.append(serializeExpressionPlaceholders(expression))
    }

    private fun serializeExpressionPlaceholders(expression: QueryExpression): String {
        return when (expression) {
            is BinaryQueryNode -> {
                when (expression.binaryQueryOperator) {
                    BinaryQueryOperator.and -> {
                        "(${serializeExpressionPlaceholders(expression.expressionFirst)}) AND " +
                                "(${serializeExpressionPlaceholders(expression.expressionSecond)})"
                    }
                    BinaryQueryOperator.or -> {
                        "(${serializeExpressionPlaceholders(expression.expressionFirst)}) OR " +
                                "(${serializeExpressionPlaceholders(expression.expressionSecond)})"
                    }
                }
            }
            is UnaryQueryNode -> {
                when (expression.unaryQueryOperator) {
                    UnaryQueryOperator.not -> {
                        "not(${serializeExpressionPlaceholders(expression.expression)})"
                    }
                }
            }
            is QueryTypeLeafCondition<*> -> {
                expression.toStringSQL()
            }
            is QueryInformationLeafCondition<*> -> {
                expression.toStringSQL()
            }
            else -> {
                logger.error("Unknown Query leaf with class -- ${expression::class.qualifiedName}.")
                ""
            }
        }
    }

    private fun addExpressionValues(expression: QueryExpression, statement: PreparedStatement, dbIndex: Int) : Int {
        var resultDbIndex = dbIndex
        when (expression) {
            is BinaryQueryNode -> {
                resultDbIndex = addExpressionValues(expression.expressionFirst, statement, resultDbIndex)
                resultDbIndex = addExpressionValues(expression.expressionSecond, statement, resultDbIndex)
            }
            is UnaryQueryNode -> {
                resultDbIndex = addExpressionValues(expression.expression, statement, resultDbIndex)
            }
            is QueryTypeLeafCondition<*> -> {
                resultDbIndex = statement.setColumn(expression.valueType, expression.value, resultDbIndex)
            }
            is QueryInformationLeafCondition<*> -> {
                resultDbIndex = statement.setColumn(expression.valueType, expression.value, resultDbIndex)
            }
            else -> {
                logger.error("Unknown Query leaf with class -- ${expression::class.qualifiedName}.")
            }
        }
        return resultDbIndex
    }

    /**
     * Every append required to end by space to construct right expression with this function
     */
    private fun addLimitPlaceholders(limits: QueryParameters.LimitParameters, sqlSelect: StringBuilder) {
        if (limits.isLimited) {
            sqlSelect.append("LIMIT ?, ? ")
        }
    }

    private fun setLimitValues(limits: QueryParameters.LimitParameters, preparedStatement: PreparedStatement?, dbIndex: Int): Int {
        if (limits.isLimited && preparedStatement != null) {
            preparedStatement.setInt(dbIndex, limits.limitStart)
            preparedStatement.setInt(dbIndex + 1, limits.limitLength)
            return dbIndex + 2
        }
        return dbIndex
    }

    /**
     * Every append required to end by space to construct right expression with this function
     */
    private fun addOrderBy(orderBy: QueryParameters.OrderByParameters, sqlExpression: StringBuilder) {
        val allOrder = orderBy.codesState.map { it.key.getCode() to it.value }.toMutableList()
        allOrder.addAll(orderBy.codesInformation.map { it.key.code to it.value })
        if (orderBy.isOrdered) {
            sqlExpression.append("ORDER BY ${allOrder.joinToString { "${it.first} ${it.second.toStringSQL()}" }} ")
        }
    }

    private fun QueryParameters.OrderByParameters.Order.toStringSQL(): String {
        return when (this) {
            QueryParameters.OrderByParameters.Order.ASC -> {
                "ASC "
            }
            QueryParameters.OrderByParameters.Order.DESC -> {
                "DESC "
            }
        }
    }

    private fun PreparedStatement.setColumn(type: InnerType, value: Any, dbIndex: Int) : Int {
        var resultDbIndex = dbIndex
        if (value is List<*>) {
            for (entity in value) {
                resultDbIndex = this.setColumn(type, entity!!, dbIndex)
            }
        } else {
            when (type) {
                InnerType.Long -> {
                    this.setLong(dbIndex, value as Long)
                }
                InnerType.ULong -> {
                    this.setLong(dbIndex, value as Long)
                }
                InnerType.String -> {
                    this.setString(dbIndex, value as String)
                }
                InnerType.Boolean -> {
                    this.setByte(dbIndex, if (value as Boolean) 1 else 0)
                }
                InnerType.Date -> {
                    this.setDate(dbIndex, (value as java.util.Date).toSqlDate())
                }
                InnerType.DateTime -> {
                    this.setTimestamp(dbIndex, (value as DateTime).toSqlTimestamp())
                }
            }
            resultDbIndex += 1
        }
        return resultDbIndex
    }
}
