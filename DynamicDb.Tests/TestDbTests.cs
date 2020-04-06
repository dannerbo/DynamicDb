using System;
using System.Data.SqlClient;
using System.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static DynamicDb.Tests.DynamicDbTests;

namespace DynamicDb.Tests
{
	[TestClass]
	public class TestDbTests
	{
		[TestCleanup]
		public void TestCleanup()
		{
			using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
			{
				connection.Open();

				using (var command = new SqlCommand(@"
					DELETE dbo.Address
					DELETE dbo.Person

					DBCC CHECKIDENT ('dbo.Address', reseed, 0)
					DBCC CHECKIDENT ('dbo.Person', reseed, 0)",
					connection))
				{
					command.ExecuteNonQuery();
				}
			}
		}

		[TestMethod]
		public void Insert_DeleteOnDispose_InsertedRecordsAreDeletedWhenDisposed()
		{
			var personRecords = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 50,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			var addressRecords = new dynamic[]
			{
				new
				{
					PersonId = 1,
					Street = "123 Fake St.",
					Apt = (string)null,
					City = "Nuketown",
					State = "WI",
					ZipCode = "51234"
				}
			};

			using (var testDb = new TestDb(DynamicDbTests.DbConnectionString))
			{
				var insertedPersonRecords = testDb.Insert("dbo.Person", true, personRecords);
				var insertedAddressRecords = testDb.Insert("dbo.Address", true, addressRecords);

				Assert.AreEqual(1, insertedPersonRecords.Length);
				Assert.AreEqual(1, insertedPersonRecords[0].Id);
				DynamicDbTests.AssertPersonRecordMatches(personRecords[0], insertedPersonRecords[0]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(personRecords);
			}

			DynamicDbTests.AssertPersonRecordsDoNotExist(1);
			DynamicDbTests.AssertAddressRecordsDoNotExist(1);
		}

		[TestMethod]
		public void Insert_RollbackWithTransactionScope_RecordsAreRolledBackWhenDisposed()
		{
			var personRecords = new dynamic[]
{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 50,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			var addressRecords = new dynamic[]
			{
				new
				{
					PersonId = 1,
					Street = "123 Fake St.",
					Apt = (string)null,
					City = "Nuketown",
					State = "WI",
					ZipCode = "51234"
				}
			};

			using (var testDb = new TestDb(DynamicDbTests.DbConnectionString, rollbackWithTransactionScope: true))
			{
				var insertedPersonRecords = testDb.Insert("dbo.Person", personRecords);
				var insertedAddressRecords = testDb.Insert("dbo.Address", addressRecords);

				Assert.AreEqual(1, insertedPersonRecords.Length);
				Assert.AreEqual(1, insertedPersonRecords[0].Id);
				DynamicDbTests.AssertPersonRecordMatches(personRecords[0], insertedPersonRecords[0]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(personRecords);
			}

			DynamicDbTests.AssertPersonRecordsDoNotExist(1);
			DynamicDbTests.AssertAddressRecordsDoNotExist(1);
		}
	}
}
