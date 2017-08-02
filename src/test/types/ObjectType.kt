package types

import javassist.bytecode.SignatureAttribute
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.Test
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.model.external.records.ObjectState
import tanvd.audit.model.external.types.objects.ObjectPresenter
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.external.types.objects.StateType
import utils.TypeUtils

internal class AddInformationTypeTest {



    open class First

    object FirstPresenter : ObjectPresenter<First>() {
        override val useDeserialization: Boolean = true
        override val entityName: String = "First"

        override val fieldSerializers: Map<StateType<*>, (First) -> String> = emptyMap()

        override val deserializer: (ObjectState) -> First? =  { First()}

    }

    class FirstChild : First()

    class Second

    @AfterMethod
    fun clean() {
        TypeUtils.clearTypes()
    }

    @Test
    fun resolveType_foundTypeDirectly_typeReturned() {
        val type = ObjectType(First::class, FirstPresenter) as ObjectType<Any>
        ObjectType.addType(type)
        val resultType = ObjectType.resolveType(First::class)
        Assert.assertEquals(resultType, type)
    }

    @Test
    fun resolveType_foundTypeIndirectly_typeReturned() {
        val type = ObjectType(First::class, FirstPresenter) as ObjectType<Any>
        ObjectType.addType(type)
        val resultType = ObjectType.resolveType(FirstChild::class)
        Assert.assertEquals(resultType, type)
    }

    @Test
    fun resolveType_notFoundType_exceptionThrown() {
        val type = ObjectType(First::class, FirstPresenter) as ObjectType<Any>
        ObjectType.addType(type)
        try {
            ObjectType.resolveType(Second::class)
        } catch(e: UnknownObjectTypeException) {
            return
        }
        Assert.fail()
    }
}
