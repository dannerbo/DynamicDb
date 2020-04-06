# DynamicDb
DynamicDb is a lightweight library with a simple interface that can be used to peform common SQL database operations.  Dynamic SQL is generated behind the scenes and results are mapped to dynamic objects (if applicable).  DynamicDb is best used for unit testing and utility-type applications.

## NuGet Packages
| Package | NuGet Stable | NuGet Pre-release | Downloads |
| ------- | ------------ | ----------------- | --------- |
| [DynamicDb](https://www.nuget.org/packages/DynamicDb/) | [![DynamicDb](https://img.shields.io/nuget/v/DynamicDb.svg)](https://www.nuget.org/packages/DynamicDb/) | [![DynamicDb](https://img.shields.io/nuget/vpre/DynamicDb.svg)](https://www.nuget.org/packages/DynamicDb/) | [![DynamicDb](https://img.shields.io/nuget/dt/DynamicDb.svg)](https://www.nuget.org/packages/DynamicDb/) |

## Usage

### Select all rows from a table

In this example we will select all rows from the **dbo.Person** table and print them to the console.  Make notice that dynamic records are returned with all the fields from the table.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Select("dbo.Person");

    foreach (var record in records)
    {
        Console.WriteLine($"{record.Id}, {record.FirstName}, {record.LastName}");
    }
}
```

### Select filtered rows from a table

In this example we will select all **dbo.Person** records where the **FirstName** is equal to **John**.  Make notice that criteria can be provided via anonymous types.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Select("dbo.Person", new { FirstName = "John" });
}
```

### Select filtered rows from a table via multiple criteria

In this example we will select all **dbo.Person** records where the **FirstName** is equal to **John** or the **LastName** is equal to **Doe** and **Age** is equal to **40**.  Each criteria object translates to an **OR** condition, and each condition within each criteria object tranlsates to an **AND** condition.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Select("dbo.Person", new { FirstName = "John" }, new { LastName = "Doe", Age = 40 });
}
```

### Insert rows into a table

In this example we will insert multiple **dbo.Person** records and print them to the console.  Make notice that records can be provided via anonymous types.  Also make notice that dynamic objects are returned for all the inserted records with all the fields from the table, which includes, but is not limited to identity columns, default values, values set by triggers, etc.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Insert("dbo.Person",
        new
        {
            FirstName = "John",
            LastName = "Doe",
            MiddleInitial = "A",
            Age = 50,
            DateOfBirth = DateTime.Parse("2000-01-01"),
            Gender = Gender.Male
        },
        new
        {
            FirstName = "Jane",
            LastName = "Doe",
            MiddleInitial = "M",
            Age = 40,
            DateOfBirth = DateTime.Parse("2000-01-01"),
            Gender = Gender.Female
        });

    foreach (var record in records)
    {
        // Id field is an auto-identity and CreatedDate has a default value defined in the database
        Console.WriteLine($"{record.Id}, {record.FirstName}, {record.LastName}, {record.CreatedDate}");
    }
}
```

### Update rows in a table

In this example we will update the **Age** field to **30** on all **dbo.Person** records where the first name equals **John** and print the affected records to the console.  Make notice that the new field values and criteria can be provided via anonymous types.  Also make notice that dynamic objects are returned for all the updated records with all the fields from the table with their updated values.  Multiple criteria can also be supplied.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Update("dbo.Person", new { Age = 30 }, new { FirstName = "John" });

    foreach (var record in records)
    {
        // UpdatedDate is updated by an UPDATE trigger
        Console.WriteLine($"{record.Id}, {record.FirstName}, {record.LastName}, {record.Age}, {record.UpdatedDate}");
    }
}
```

### Delete rows in a table

In this example we will delete all **dbo.Person** records where the first name equals **John** and print the affected records to the console.  Make notice that the criteria can be provided via anonymous types.  Also make notice that dynamic objects are returned for all the deleted records with all the fields from the table.  Mutliple criteria can also be supplied.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Delete("dbo.Person", new { FirstName = "John" });

    foreach (var record in records)
    {
        Console.WriteLine($"{record.Id}, {record.FirstName}, {record.LastName}");
    }
}
```

### Query all rows from a table using command text

In this example we will select all **dbo.Person** records and print them to the console.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Query("SELECT * FROM dbo.Person");

    foreach (var record in records)
    {
        Console.WriteLine($"{record.Id}, {record.FirstName}, {record.LastName}");
    }
}
```

### Query filtered rows from a table using command text and a parameter

In this example we will select all **dbo.Person** records where the minimum age is **45** by passing a **MinimumAge** parameter to the query.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Query("SELECT * FROM dbo.Person WHERE Age >= @MinimumAge", parameters: new { MinimumAge = 45 });
}
```

### Query rows from a table using a stored procedure

In this example we will select **dbo.Person** records using the **dbo.GetAllPeople** stored procedure.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Query("dbo.GetAllPeople", commandType: CommandType.StoredProcedure);
}
```

### Query filtered rows from a table using a stored procedure and a parameter

In this example we will select all **dbo.Person** records where the minimum age is **45** by passing a **MinimumAge** parameter to the stored procedure.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var records = dynamicDb.Query(
        "dbo.GetAllPeopleByMinimumAge",
        parameters: new { MinimumAge = 45 },
        commandType: CommandType.StoredProcedure);
}
```

### Query rows from multiple tables using command text

In this example we will select from multiple tables and print the results to the console.  Make notice that dynamic records are returned containing all the fields in the provided query.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var commandText = @"
        SELECT PersonId = p.Id,
            p.FirstName,
            p.LastName,
            p.MiddleInitial,
            p.Age,
            p.DateOfBirth,
            p.Gender,
            AddressId = a.Id,
            a.Street,
            a.Apt,
            a.City,
            a.State,
            a.ZipCode
        FROM dbo.Person p
        LEFT JOIN dbo.Address a ON p.Id = a.PersonId";

    var records = dynamicDb.Query(commandText);

    foreach (var record in records)
    {
        Console.WriteLine($"{record.FirstName} {record.LastName} lives in {record.City}, {record.State} at {record.Street} {record.Apt}");
    }
}
```

### Execute a non-query script and get number of affected rows

In this example we will execute a script to update **Age** to **60** for all **dbo.Person** records.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var affectedRecordsCount = dynamicDb.Execute("UPDATE dbo.Person SET Age = 60");
}
```

### Execute a non-query script with a parameter and get number of affected rows

In this example we will execute a script to update **Age** to **60** for all **dbo.Person** records using a parameter for the new **Age** value.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var affectedRecordsCount = dynamicDb.Execute("UPDATE dbo.Person SET Age = @Age", parameters: new { Age = 60 });
}
```

### Execute a non-query stored procedure and get number of affected rows

In this example we will execute a stored procedure and get the number of affected records.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var affectedRecordsCount = dynamicDb.Execute("dbo.UpdateAllAgesTo60", commandType: CommandType.StoredProcedure);
}
```

### Execute a non-query stored procedure with a parameter and get number of affected rows

In this example we will execute a stored procedure with a parameter and get the number of affected records.

```csharp
using (var dynamicDb = new DynamicDb(connectionStringOrConnection))
{
    var affectedRecordsCount = dynamicDb.Execute(
        "dbo.UpdateAllAges",
        parameters: new { Age = 60 },
        commandType: CommandType.StoredProcedure);
}
```

### Write a unit test for a DAL repository

In this example we will write a unit test to verify an **Insert** method for a repository which requires a foreign record is inserting records correctly.  We will be using the **TestDb** class which inherits from **DynamicDb** so we can use the **Insert** overload that allows us to specify that we want the data we insert to be deleted when the **TestDb** object is disposed.

```csharp
[TestMethod]
public void Insert_InsertAddressForExistingPerson_AddressIsInserted()
{
    // Connection or connection string should be for a "unit tester" user who permissions to perform the TestDb actions
    using (var testDb = new TestDb(connectionStringOrConnectionForUnitTestUser))
    {
        // Insert temporary dbo.Person record that will be referenced by the dbo.Address record that we're going to insert via the repository
        var person = testDb.Insert("dbo.Person",
            true, // Specify true for deleteOnDispose so these records are automatically deleted when TestDb is disposed
            new
            {
                FirstName = "John",
                LastName = "Doe",
                MiddleInitial = "A",
                Age = 50,
                DateOfBirth = DateTime.Parse("2000-01-01"),
                Gender = Gender.Male
            }).Single();

        var address = new Models.Address()
        {
            PersonId = person.Id, // Auto-identity from inserted dbo.Person record above
            Street = "123 Fake St.",
            City = "Nuketown",
            State = "WI",
            ZipCode = "51234"
        };

        // Connection or connection string should be for the app user so permissions can be tested
        var addressRepository = new AddressRepository(connectionStringOrConnectionForAppUser);
        
        addressRepository.Insert(address);
        
        // Delete and return the record we expect to have been inserted
        var addressRecord = testDb.Delete("dbo.Address", new { PersonId = person.Id }).Single();
        
        Assert.AreEqual(address.Street, addressRecord.Street);
        Assert.AreEqual(address.City, addressRecord.City);
        Assert.AreEqual(address.State, addressRecord.State);
        Assert.AreEqual(address.ZipCode, addressRecord.ZipCode);
        Assert.IsNull(addressRecord.Apt); // This field was not provided so it should be NULL
    }
}
```

### Write a unit test for a DAL repository that rolls back all db changes on disposal

Like the previous example, we will write a unit test to verify an **Insert** method for a repository which requires a foreign record is inserting records correctly.  The difference being that this example tells **TestDb** to use a **TransactionScope** to rollback any changes when the **TestDb** object is disposed.  Make notice that **true** is no longer specified for the **deleteOnDispose** argument to the **Insert** method.  Also, we no longer use the **Delete** method to delete the data inserted by the DAL repository since the data won't be committed.

```csharp
[TestMethod]
public void Insert_InsertAddressForExistingPerson_AddressIsInserted()
{
    // rollbackWithTransactionScope argument is used to rollback all db changes when TestDb object is disposed
    using (var testDb = new TestDb(connectionStringOrConnectionForUnitTestUser, rollbackWithTransactionScope: true))
    {
        var person = testDb.Insert("dbo.Person",
            new
            {
                FirstName = "John",
                LastName = "Doe",
                MiddleInitial = "A",
                Age = 50,
                DateOfBirth = DateTime.Parse("2000-01-01"),
                Gender = Gender.Male
            }).Single();

        var address = new Models.Address()
        {
            PersonId = person.Id,
            Street = "123 Fake St.",
            City = "Nuketown",
            State = "WI",
            ZipCode = "51234"
        };

        var addressRepository = new AddressRepository(connectionStringOrConnectionForAppUser);
        
        addressRepository.Insert(address);
        
        var addressRecord = testDb.Select("dbo.Address", new { PersonId = person.Id }).Single();
        
        Assert.AreEqual(address.Street, addressRecord.Street);
        Assert.AreEqual(address.City, addressRecord.City);
        Assert.AreEqual(address.State, addressRecord.State);
        Assert.AreEqual(address.ZipCode, addressRecord.ZipCode);
        Assert.IsNull(addressRecord.Apt);
    }
}
```

### Write a unit test for a DAL repository with user-defined TestDb extension methods

In this example we use user-defined **TestDb** extension methods for better code re-use and unit test readability.  The **Address** model generation and assertions could also be moved to separate methods for re-use and readability.

```csharp
[TestMethod]
public void Insert_InsertAddressForExistingPerson_AddressIsInserted()
{
    using (var testDb = new TestDb(connectionStringOrConnectionForUnitTestUser, rollbackWithTransactionScope: true))
    {
        // Insert temporary dbo.Person record with extension method
        var person = testDb.InsertPerson();

        var address = new Models.Address()
        {
            PersonId = person.Id,
            Street = "123 Fake St.",
            City = "Nuketown",
            State = "WI",
            ZipCode = "51234"
        };

        var addressRepository = new AddressRepository(connectionStringOrConnectionForAppUser);
        
        addressRepository.Insert(address);
        
        // Select the record we expect to have been inserted with extension method
        var addressRecord = testDb.SelectAddress(person.Id);
        
        Assert.AreEqual(address.Street, addressRecord.Street);
        Assert.AreEqual(address.City, addressRecord.City);
        Assert.AreEqual(address.State, addressRecord.State);
        Assert.AreEqual(address.ZipCode, addressRecord.ZipCode);
        Assert.IsNull(addressRecord.Apt);
    }
}

public static dynamic InsertPerson(this TestDb testDb)
{
    return testDb.Insert("dbo.Person",
        new
        {
            FirstName = "John",
            LastName = "Doe",
            MiddleInitial = "A",
            Age = 50,
            DateOfBirth = DateTime.Parse("2000-01-01"),
            Gender = Gender.Male
        }).Single();
}

public static dynamic SelectAddress(this TestDb testDb, long personId)
{
    return testDb.Select("dbo.Address", new { PersonId = personId }).Single();
}
```
