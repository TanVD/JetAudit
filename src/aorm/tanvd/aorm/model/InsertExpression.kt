package tanvd.aorm.model

data class InsertValues(val values: Map<Column<Any>, Any>)
data class InsertExpression(val table: Table, val columns: List<Column<Any>>, val values: MutableList<InsertValues>)

//Helper functions

infix fun Table.insert(values: List<Pair<Column<*>, Any>>) {
    return this.insert(InsertExpression(this, values.map { it.first as Column<Any> },
            mutableListOf(InsertValues(values.toMap() as Map<Column<Any>, Any>))))
}

infix fun Table.insertWithColumns(columns: List<Column<*>>): InsertExpression {
    return InsertExpression(this, columns as List<Column<Any>>, ArrayList())
}

infix fun InsertExpression.values(values: List<List<Pair<Column<*>, Any>>>) {
    this.values.addAll(values.map { InsertValues(it.toMap() as Map<Column<Any>, Any>) })
    this.table.insert(this)
}
