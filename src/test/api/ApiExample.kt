//package tanvd.audit.test.api
//
//import org.testng.annotations.AfterMethod
//import org.testng.annotations.BeforeMethod
//import tanvd.aorm.DbLong
//import tanvd.aorm.query.Order
//import tanvd.aorm.query.OrderByExpression
//import tanvd.aorm.query.and
//import tanvd.audit.AuditAPI
//import tanvd.audit.implementation.clickhouse.aorm.AuditTable
//import tanvd.audit.model.external.presenters.TimeStampType
//import tanvd.audit.model.external.queries.equal
//import tanvd.audit.model.external.queries.less
//import tanvd.audit.model.external.records.AuditRecord
//import tanvd.audit.model.external.records.InformationObject
//import tanvd.audit.model.external.records.ObjectState
//import tanvd.audit.model.external.types.information.InformationType
//import tanvd.audit.model.external.types.objects.*
//import utils.DbUtils
//import utils.TypeUtils
//import java.text.SimpleDateFormat
//import java.time.Instant
//import java.util.*
//import kotlin.properties.Delegates
//
//internal class AuditApiExample {
//
//    object OrderPresenter : ObjectPresenter<Order>() {
//        override val useDeserialization: Boolean = true
//
//        override val entityName: String = "Order"
//
//        val id = string("Id")
//
//        override val fieldSerializers: Map<StateType<*>, (Order) -> String> = hashMapOf(id to { value -> value.id })
//        override val deserializer: (ObjectState) -> Order? = { (stateList) -> Order(stateList[id] as String) }
//
//    }
//
//    class Order(val id: String) {
//        override fun toString(): String {
//            return "Order: " + id
//        }
//    }
//
//    object AccountPresenter : ObjectPresenter<Account>() {
//        override val useDeserialization: Boolean = true
//
//        override val entityName: String = "Account"
//
//        val id = string("Id")
//
//        override val fieldSerializers: Map<StateType<*>, (Account) -> String> = hashMapOf(id to { value -> value.id })
//        override val deserializer: (ObjectState) -> Account? = { (stateList) -> Account(stateList[id] as String) }
//
//    }
//
//    class Account(val id: String) {
//        override fun toString(): String {
//            return "Account: " + id
//        }
//    }
//
//    private var auditApi: AuditAPI by Delegates.notNull<AuditAPI>()
//
//    @BeforeMethod
//    fun addTypes() {
//        auditApi = AuditAPI(DbUtils.getProperties())
//        auditApi.addObjectType(ObjectType(Order::class, OrderPresenter))
//        auditApi.addObjectType(ObjectType(Account::class, AccountPresenter))
//    }
//
//    @AfterMethod
//    fun clear() {
//        AuditTable.drop()
//        TypeUtils.clearTypes()
//    }
//
//    private fun printTime(time: Long): String {
//        val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        return dateFormat.format(Date.from(Instant.ofEpochMilli(time)))
//    }
//
//    //    @Test
//    fun simpleSaveAndLoad() {
//
//        val accountFirst = Account("John Doe")
//        val order = Order("Cool order")
//        auditApi.save(accountFirst, " ordered ", order)
//        auditApi.commit()
//
//        Thread.sleep(5000)
//
//        val records = auditApi.load(AccountPresenter.id equal "John Doe")
//
//        System.out.println("Found ${records.size} records")
//
//        System.out.println("Printing first record: ")
//
//        System.out.println(records[0].objects.joinToString(separator = " ") { it.obj?.toString() ?: "Unknown entity" })
//
//        System.out.println("Time: ${printTime(records[0].getInformationValue(TimeStampType)!!)}")
//    }
//
//    object TitleInf : InformationType<Long>("TitleInformation", DbLong()) {
//        override fun getDefault(): Long {
//            return -1
//        }
//
//    }
//
//    //    @Test
//    fun usageOfInformationAndOrderBy() {
//
//        auditApi.addInformationType(TitleInf)
//
//        val accountFirst = Account("John Doe")
//        val order = Order("Cool order")
//        for (i in 1..20000L) {
//            val title = InformationObject(i, TitleInf)
//            auditApi.save(accountFirst, " ordered ", order, information = setOf(title))
//        }
//        auditApi.commit()
//
//        Thread.sleep(5000)
//
//        val records = auditApi.load((AccountPresenter.id equal "John Doe") and
//                (TitleInf less 15000) and (TitleInf more 14995), orderByExpression = OrderByExpression(TitleInf to DESC))
//
//        System.out.println("Found ${records.size} records")
//
//        for (i in 0..(records.size - 1)) {
//            System.out.println("Printing $i record: ")
//
//            System.out.println(records[i].objects.joinToString(separator = " ") { it.obj?.toString() ?: "Unknown entity" } + " " +
//                    "Title: " + records[i].getInformationValue(TitleInf))
//
//            System.out.println("Time: ${printTime(records[i].getInformationValue(TimeStampType)!!)}")
//        }
//    }
//
//    object IsExternalPresenter : InformationBooleanPresenter() {
//        override val code: String = "IsExternalPresenter"
//
//        //for all already created false is default anyway
//        override fun getDefault(): Boolean {
//            return false
//        }
//
//    }
//
//    //Please, use it with extreme caution
////    @Test
//    fun updatingOfInformation() {
//
//        //create initial
//        val typeTitle = InformationType(TitleInf, InnerType.Long)
//        auditApi.addInformationType(typeTitle)
//
//        val accountFirst = Account("John Doe")
//        val order = Order("Cool order")
//        for (i in 1..20000L) {
//            val title = InformationObject(i, TitleInf)
//            auditApi.save(accountFirst, " ordered ", order, information = setOf(title))
//        }
//        auditApi.commit()
//
//        Thread.sleep(5000)
//
//        val typeIsExternal = InformationType(IsExternalPresenter, InnerType.Boolean)
//        auditApi.addInformationType(typeIsExternal)
//
//
//        val parameters = QueryParameters()
//        parameters.setInformationOrder(TitleInf to DESC)
//        val records = auditApi.load((AccountPresenter.id equal "John Doe") and
//                ((TitleInf less 15000) and (TitleInf more 14995)), parameters)
//
//        //print initial
//
//        System.out.println("Found ${records.size} records")
//
//        for (i in 0..(records.size - 1)) {
//            System.out.println("Printing $i record: ")
//
//            System.out.println(records[i].objects.joinToString(separator = " ") { it.obj?.toString() ?: "Unknown entity" } + " " +
//                    "Title: " + records[i].getInformationValue(TitleInf) + " " +
//                    "IsExternal: " + records[i].getInformationValue(IsExternalPresenter))
//
//            System.out.println("Time: ${printTime(records[i].getInformationValue(TimeStampType)!!)}")
//        }
//
//        //change is external to true
//
//        val updatedRecords = ArrayList<AuditRecord>()
//        for (record in records) {
//            record.informations.removeIf { it.type.code == IsExternalPresenter.code }
//            record.informations.add(InformationObject(true, IsExternalPresenter))
//            updatedRecords.add(record)
//        }
//
//        auditApi.replace(updatedRecords)
//
//        Thread.sleep(5000)
//
//        //print new
//
//        val resultingRecords = auditApi.load((AccountPresenter.id equal "John Doe") and
//                ((TitleInf less 15000) and (TitleInf more 14995)), parameters)
//
//        System.out.println("Found ${records.size} records")
//
//        for (i in 0..(resultingRecords.size - 1)) {
//            System.out.println("Printing $i record: ")
//
//            System.out.println(resultingRecords[i].objects.joinToString(separator = " ") { it.obj?.toString() ?: "Unknown entity" } + " " +
//                    "Title: " + records[i].getInformationValue(TitleInf) + " " +
//                    "IsExternal: " + records[i].getInformationValue(IsExternalPresenter))
//
//            System.out.println("Time: ${printTime(resultingRecords[i].getInformationValue(TimeStampType)!!)}")
//        }
//    }
//}
//
