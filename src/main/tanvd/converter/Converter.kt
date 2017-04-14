package tanvd.converter

import tanvd.audit.implementation.clickhouse.model.DbRow

internal interface Converter {
    fun convertToDbRow(strings: List<String>): DbRow
}