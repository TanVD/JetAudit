package test

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.AuditAPI
import tanvd.audit.model.AuditSerializer
import tanvd.audit.model.AuditType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.LongSerializer
import java.util.*


internal class AuditSave {

    data class EmpireStateBuilding(val rooms : Int = 1001) {
        companion object serializer : AuditSerializer<EmpireStateBuilding> {
            override fun deserialize(serializedString: String): EmpireStateBuilding {
                if (serializedString == "EmpireStateBuilding") {
                    return EmpireStateBuilding()
                }
                return EmpireStateBuilding(-1)
            }

            override fun serialize(value: EmpireStateBuilding): String {
                return "EmpireStateBuilding"
            }

        }
    }

    //Bad test, but ok for now
    @Test
    fun testSave() {
        val auditApi = AuditAPI("jdbc:mysql://localhost/example?useLegacyDatetimeCode=false" +
                "&serverTimezone=Europe/Moscow", "root", "root")
        auditApi.addTypeForAudit(AuditType(Int::class, "Type_Int", IntSerializer))
        auditApi.addTypeForAudit(AuditType(Long::class, "Type_Long", LongSerializer))
        auditApi.addTypeForAudit(AuditType(EmpireStateBuilding::class, "Type_EmpireStateBuilding",
                EmpireStateBuilding))
        val arrayList = arrayOf(123L, "got", 456, "dollars from the", EmpireStateBuilding())
        auditApi.saveAudit(*arrayList)
        Thread.sleep(10000)
        val result = auditApi.loadAudit(AuditType.TypesResolution.resolveType(Long::class), "123")
        Assert.assertEquals(result[0], ArrayList(arrayList.toMutableList()))
    }

    @Test
    fun testSave1() {
        val auditApi = AuditAPI("jdbc:clickhouse://localhost:8123/example", "default", "")
        auditApi.addTypeForAudit(AuditType(Int::class, "Type_Int", IntSerializer))
        auditApi.addTypeForAudit(AuditType(Long::class, "Type_Long", LongSerializer))
        auditApi.addTypeForAudit(AuditType(EmpireStateBuilding::class, "Type_EmpireStateBuilding",
                EmpireStateBuilding))
        val arrayList = arrayOf(123L, "got", 456, "dollars from the", EmpireStateBuilding())
        auditApi.saveAudit(*arrayList)
        Thread.sleep(20000)
        val result = auditApi.loadAudit(AuditType.TypesResolution.resolveType(Long::class), "123")
        Assert.assertEquals(result[0], ArrayList(arrayList.toMutableList()))
    }

    @Test
    fun testSave2() {
        val auditApi = AuditAPI("jdbc:clickhouse://localhost:8123/example", "default", "")
        auditApi.addTypeForAudit(AuditType(Int::class, "Int", IntSerializer))
        auditApi.addTypeForAudit(AuditType(EmpireStateBuilding::class, "Type_EmpireStateBuilding",
                EmpireStateBuilding))
    }
}
