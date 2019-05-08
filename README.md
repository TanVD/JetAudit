# JetAudit

[ ![Download](https://api.bintray.com/packages/tanvd/jetaudit/jetaudit/images/download.svg) ](https://bintray.com/tanvd/jetaudit/jetaudit/_latestVersion)
[![CircleCI](https://circleci.com/gh/TanVD/JetAudit.svg?style=svg)](https://circleci.com/gh/TanVD/JetAudit)

JetAudit is library for business process audit. It uses ClickHouse as a data storage for audit events.

Basically, it is an interface to save and load different events and do it under heavy load and very fast.

## Events

Events, in terms of JetAudit, are lists of objects and some information relevant to them. Mostly, it suits for business-process audit needs.

For example, you have an object of type "Contract" and object of type "Customer". You can save following event:

```
Audit.save(customer, "paid for", contract);
```

Every object will be mapped in accordance with it's type (see [type systems paragraph](#type-system) ) and asynchronously saved to ClickHouse.

## Setup

JetAudit releases are published to [JCenter](https://bintray.com/tanvd/jetaudit/jetaudit).

To use JetAudit you'll need ClickHouse installation (we recommend to use replicated cluster in production).

Setup properties file in accordance with a documentation of AuditApi class. Once it is done, you can try to save your first audit record. Note, that by default JetAudit type system initialized only with primitive types.

## How to

First of all you'll need to define your own set of types. Consult [type systems paragraph](#type-system) paragraph for how to.

Once it is done, you'll need to create AuditAPI object (it is strongly recommended instantiate it as singleton). 

Once AuditAPI object is created, it will start AuditWorkers, which are used to asynchronously save records.

After it you can save records:

`AuditAPI.save(customer, "have bought", 15, "boats", "from", seller)`

or even

`AuditAPI.save(customer, SellEvent.buy, boats, seller)` (we assume that `boats: List<Boat>`) 

It all depends on your type system!

Also you can load records.

`AuditAPI.load((Customer.id eq 15) and (SellEvent.type eq SellEvent.buy.name) and (Seller.id eq 21))`

Note, that format of loaded records depends on your type definitions. You can enable deserialization and add deserializers to every ObjectType and then you'll get exactly the data you've been saving (of course, if your deserializers works right).
 
Otherwise, you can disable serialization, and you'll get back only saved state of each audit record object. In that case you'll have no overhead on deserialization and will load records very fast.

Depending on a situation one or another load format may suit your needs. 

## Type system

Every object saved using JetAudit should be added to it's type system. There are 2 general types - ObjectType and InformationType.

### Object type

ObjectType should be used for objects, that can occur more than one time in audit record. For example, it is customers (one customer may buy something from another). Every ObjectPresenter (presenters implements logic of ObjectType and works as interface for load of events) may have few serializers and appropriate StateTypes. When event is saved, serializers are invoked on an object, and their results are saved into ClickHouse, each to defined by StateType column.

Let's see it working:

```
data class TestClass(val id: String, val name: String)

object TestClassPresenter : ObjectPresenter<TestClass>() {
    override val useDeserialization: Boolean = true

    override val entityName: String = "TestClassString"

    val id = string("Id") { it.id }
    val name = string("Name") { it.name }

    override val deserializer: (ObjectState) -> TestClass? = { (stateList) ->
        if (stateList[id] == null) null else TestClass(stateList[id] as String, stateList[name] as String)
    }
}

Audit.addObjectType(ObjectType(TestClass::class, TestClassPresenter))
```

Here is a simple example. When object of type TestClass will be saved, as part of audit event, JetAudit will find corresponding ObjectType in type system (TestClassPresenter implements it) and will serialize it to 2 fields - "TestClass_Id" with value from id field, and "TestClass_Name" with value from name field. Here `val id` and `val name` are StateTypes with serializers, and TestClassPresenter is an object implementing all the logic for TestClass ObjectType.

Note, that "TestClass_Id" and "TestClass_Name" will be also columns used in ClickHouse to save objects of this ObjectType. Columns will be of type `Array(String)`.

### Information type

Information type should be used for objects, that may occur only once in audit record. For example, it is a timestamp of record itself. 

InformationTypes are very simple, cause their values maps right into columns of Clickhouse.

Here is a definition of a simple information type:

`object LongInf : InformationType<Long>("LongInfColumn", DbInt64(), { 0 })`

Note, that values of this type will be mapped to column LongInfColumn in Clickhouse.

## Ops notes

We strongly encourage you to use replicated ClickHouse setup. 

JetAudit requires *ReplacingMergeTree engine family. It writes a version of audit record, and you can delete some records just writing new empty one with later version and equal id and timestamp

JetAudit may align it's own scheme even in replicated setup, but we recommend you to disable defaultDDL in production and align the scheme manually, so you can react on any errors ClickHouse may produce.

## More examples

More examples you may find in JetAudit tests. Also, feel free to contact me (@tanvd) with any questions on JetAudit usage.



