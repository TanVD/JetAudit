package tanvd.jetaudit.types

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.Test
import tanvd.jetaudit.exceptions.UnknownObjectTypeException
import tanvd.jetaudit.model.external.records.ObjectState
import tanvd.jetaudit.model.external.types.objects.ObjectPresenter
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.utils.TestUtil

@Suppress("UNCHECKED_CAST")
internal class AddInformationTypeTest {

    open class First

    object FirstPresenter : ObjectPresenter<First>() {
        override val useDeserialization: Boolean = true
        override val entityName: String = "First"

        override val deserializer: (ObjectState) -> First? = { First() }

    }

    class FirstChild : First()

    class Second

    @AfterMethod
    fun clean() {
        TestUtil.drop()
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
        } catch (e: UnknownObjectTypeException) {
            return
        }
        Assert.fail()
    }
}
