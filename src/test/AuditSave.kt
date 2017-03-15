package test

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.AuditAPI
import java.util.*


internal class AuditSave {

    data class EmpireStateBuilding(val rooms : Int = 1001) {
        companion object {
            fun fromString(str : String) : EmpireStateBuilding? {
                if (str == "EmpireStateBuilding") {
                    return EmpireStateBuilding()
                }
                return null
            }
        }

        override fun toString() : String {
            return "EmpireStateBuilding"
        }
    }

    //Bad test, but ok for now
    @Test
    fun testSave() {
        val auditApi = AuditAPI()
        auditApi.addTypeForAudit(Int::class, Any::toString, String::toInt)
        auditApi.addTypeForAudit(Long::class, Any::toString, String::toLong)
        auditApi.addTypeForAudit(EmpireStateBuilding::class, Any::toString,
                {x -> EmpireStateBuilding.fromString(x)!!})
        val arrayList = arrayOf(123L, "got", 456, "dollars from the", EmpireStateBuilding())
        auditApi.saveAudit(*arrayList)
        Thread.sleep(10000)
        val result = auditApi.loadAudit(Long::class, "123")
        Assert.assertEquals(result[0], ArrayList(arrayList.toMutableList()))
    }
}
