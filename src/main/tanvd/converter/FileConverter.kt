package tanvd.converter

import tanvd.converter.jetprofile.JetProfileConverter
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.PrintWriter


internal class FileConverter(
        val types: List<String>,
        val filePathFrom: String, val filePathTo: String, val fileExceptions: String,
        val totalNumberOfRecords: Int, val portion: Int) {

    fun generateScript() {
        val exceptionsWriter = PrintWriter(fileExceptions)
        val converter: Converter = JetProfileConverter()
        val reader = BufferedReader(InputStreamReader(FileInputStream(filePathFrom)))
        var times = 1

        var writer = ClickhouseSqlRowWriter(filePathTo, types)
        writer.writeHeader()

        for (i in 1..totalNumberOfRecords) {
            val wholeString = reader.readLine() ?: break
            val array = wholeString.split('\t')

            try {
                val record = converter.convertToDbRow(array)
                writer.write(record)
            } catch (e: Throwable) {
                exceptionsWriter.println(wholeString)
            }
            if (i % portion == 0) {
                writer.close()
                writer = ClickhouseSqlRowWriter(filePathTo.replaceAfterLast('.', times.toString() + "."), types)
                writer.writeHeader()
                times++
            }
        }
        writer.close()


    }

}