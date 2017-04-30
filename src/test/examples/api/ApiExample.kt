package examples.api

import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import tanvd.audit.AuditAPI
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.records.AuditRecord
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditSerializer
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.InformationPresenter
import tanvd.audit.model.external.types.InformationType
import utils.TypeUtils
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.*

internal class AuditApiExample {

    class Order(val id: String) {

        companion object serializer : AuditSerializer<Order> {
            override fun deserialize(serializedString: String): Order {
                return Order(serializedString)
            }

            override fun serialize(entity: Order): String {
                return entity.id
            }
        }

        override fun toString(): String {
            return "Order: " + id
        }
    }

    class Account(val id: String) {
        companion object serializer : AuditSerializer<Account> {
            override fun deserialize(serializedString: String): Account {
                return Account(serializedString)
            }

            override fun serialize(entity: Account): String {
                return entity.id
            }
        }

        override fun toString(): String {
            return "Account: " + id
        }
    }

    private var auditApi: AuditAPI? = null

    @BeforeMethod
    fun addTypes() {
        auditApi = AuditAPI(DbType.Clickhouse, "jdbc:clickhouse://localhost:8123/example", "default", "")
        auditApi!!.addAuditType(AuditType(Order::class, "Order", Order))
        auditApi!!.addAuditType(AuditType(Account::class, "Account", Account))

    }

    @AfterMethod
    fun clear() {
        (auditApi!!.auditDao as AuditDaoClickhouseImpl).dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
    }

    private fun printTime(time: Long): String {
        val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        return dateFormat.format(Date.from(Instant.ofEpochMilli(time)))
    }

    //@Test
    fun simpleSaveAndLoad() {

        val accountFirst = Account("John Doe")
        val order = Order("Cool order")
        auditApi!!.save(accountFirst, " ordered ", order)
        auditApi!!.commit()

        Thread.sleep(5000)

        val records = auditApi!!.load(Account::class equal accountFirst, QueryParameters())

        System.out.println("Found ${records.size} records")

        System.out.println("Printing first record: ")

        System.out.println(records[0].objects.joinToString(separator = " ") { it?.obj?.toString() ?: "Unknown entity" })

        System.out.println("Time: ${printTime(records[0].getInformationValue(TimeStampPresenter)!!)}")
    }

    object TitlePresenter : InformationPresenter<Long>() {
        override val name: String = "TitleInformation"

        override fun getDefault(): Long {
            return -1
        }

    }

    //@Test
    fun usageOfInformationAndOrderBy() {

        val typeTitle = InformationType(TitlePresenter, "TitleOfJohns", InformationType.InformationInnerType.Long)
        auditApi!!.addInformationType(typeTitle)

        val accountFirst = Account("John Doe")
        val order = Order("Cool order")
        for (i in 1..20000) {
            val title = InformationObject(i, TitlePresenter)
            auditApi!!.save(accountFirst, " ordered ", order, information = setOf(title))
        }
        auditApi!!.commit()

        Thread.sleep(5000)

        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(TitlePresenter to QueryParameters.OrderByParameters.Order.DESC))
        val records = auditApi!!.load((Account::class equal accountFirst) and
                ((TitlePresenter less 15000) and (TitlePresenter more 14995)), parameters)

        System.out.println("Found ${records.size} records")

        for (i in 0..(records.size - 1)) {
            System.out.println("Printing $i record: ")

            System.out.println(records[i].objects.joinToString(separator = " ") { it?.obj?.toString() ?: "Unknown entity" } + " " +
                    "Title: " + records[i].getInformationValue(TitlePresenter))

            System.out.println("Time: ${printTime(records[i].getInformationValue(TimeStampPresenter)!!)}")
        }
    }

    object IsExternalPresenter : InformationPresenter<Boolean>() {
        override val name: String = "IsExternalPresenter"

        //for all already created false is default anyway
        override fun getDefault(): Boolean {
            return false
        }

    }

    //Please, use it with extreme caution
    //@Test
    fun updatingOfInformation() {

        //create inital
        val typeTitle = InformationType(TitlePresenter, "TitleOfJohns", InformationType.InformationInnerType.Long)
        auditApi!!.addInformationType(typeTitle)

        val accountFirst = Account("John Doe")
        val order = Order("Cool order")
        for (i in 1..20000) {
            val title = InformationObject(i, TitlePresenter)
            auditApi!!.save(accountFirst, " ordered ", order, information = setOf(title))
        }
        auditApi!!.commit()

        Thread.sleep(5000)

        val typeIsExternal = InformationType(IsExternalPresenter, "IsExternalRecord", InformationType.InformationInnerType.Boolean)
        auditApi!!.addInformationType(typeIsExternal)


        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(TitlePresenter to QueryParameters.OrderByParameters.Order.DESC))
        val records = auditApi!!.load((Account::class equal accountFirst) and
                ((TitlePresenter less 15000) and (TitlePresenter more 14995)), parameters)

        //print inital

        System.out.println("Found ${records.size} records")

        for (i in 0..(records.size - 1)) {
            System.out.println("Printing $i record: ")

            System.out.println(records[i].objects.joinToString(separator = " ") { it?.obj?.toString() ?: "Unknown entity" } + " " +
                    "Title: " + records[i].getInformationValue(TitlePresenter) + " " +
                    "IsExternal: " + records[i].getInformationValue(IsExternalPresenter))

            System.out.println("Time: ${printTime(records[i].getInformationValue(TimeStampPresenter)!!)}")
        }

        //change is external to true

        val updatedRecords = ArrayList<AuditRecord>()
        for (record in records) {
            record.informations.removeIf { it.type.presenter.name == IsExternalPresenter.name }
            record.informations.add(InformationObject(true, IsExternalPresenter))
            updatedRecords.add(record)
        }

        auditApi!!.replace(updatedRecords)

        Thread.sleep(5000)

        //print new

        val resultingRecords = auditApi!!.load((Account::class equal accountFirst) and
                ((TitlePresenter less 15000) and (TitlePresenter more 14995)), parameters)

        System.out.println("Found ${records.size} records")

        for (i in 0..(resultingRecords.size - 1)) {
            System.out.println("Printing $i record: ")

            System.out.println(resultingRecords[i].objects.joinToString(separator = " ") { it?.obj?.toString() ?: "Unknown entity" } + " " +
                    "Title: " + records[i].getInformationValue(TitlePresenter) + " " +
                    "IsExternal: " + records[i].getInformationValue(IsExternalPresenter))

            System.out.println("Time: ${printTime(resultingRecords[i].getInformationValue(TimeStampPresenter)!!)}")
        }
    }
}

