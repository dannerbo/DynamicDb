using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DynamicDb.Tests
{
	[TestClass]
	public class DynamicDbTests
	{
		internal static readonly string DbConnectionString = ConfigurationManager.ConnectionStrings["UnitTesting"].ConnectionString;
		internal const string NonExistantDbConnectionString = "Data Source=localhost;Initial Catalog=NonExistant;Integrated Security=true;";

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
		public void Dispose_ConnectionIsProvidedViaConstructor_ConnectionIsNotDisposed()
		{
			using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
			{
				connection.Open();

				using (var dynamicDb = new DynamicDb(connection))
				{
				}

				Assert.AreEqual(ConnectionState.Open, connection.State);
			}
		}

		[TestMethod]
		public void Select_NoCriteriaAndZeroExpectedRows_ZeroRowsAreReturned()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Select("dbo.Person", criteria: null);

				Assert.AreEqual(0, selectedRecords.Length);
			}
		}

		[TestMethod]
		public void Select_NoCriteriaAndMultipleExpectedRows_RowsAreReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Select("dbo.Person", criteria: null);

				Assert.AreEqual(2, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], selectedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(records[1], selectedRecords[1]);
			}
		}

		[TestMethod]
		public void Select_CriteriaIsProvided_FilteredRowIsReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Select("dbo.Person", criteria: new { FirstName = "John" });

				Assert.AreEqual(1, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], selectedRecords[0]);
			}
		}

		[TestMethod]
		public void Select_TableWithBracketsAndCriteriaIsProvided_FilteredRowIsReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Select("[dbo].[Person]", criteria: new { FirstName = "John" });

				Assert.AreEqual(1, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], selectedRecords[0]);
			}
		}

		[TestMethod]
		public void Select_MultipleCriteriaAreProvided_FilteredRowsAreReturned()
		{
			var records = new dynamic[]
			{
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
				},

				new
				{
					FirstName = "Bruce",
					LastName = "Wayne",
					MiddleInitial = "X",
					Age = 30,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Select("dbo.Person",
					new { FirstName = "John" },
					new { FirstName = "Jane", LastName = "Doe" });

				Assert.AreEqual(2, selectedRecords.Length);

				selectedRecords.Single(x => x.FirstName == "John");
				selectedRecords.Single(x => x.FirstName == "Jane");
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Select_CriteriaWithFieldThatDoesNotExist_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Select("dbo.Person", criteria: new { NonExistantField = "whatever" });
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Select_CriteriaWithInvalidDataType_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Select("dbo.Person", criteria: new { Age = "INVALID DATA TYPE" });
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Select_TableDoesNotExist_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Select("dbo.NonExistant", criteria: null);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Select_DatabaseDoesNotExist_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.NonExistantDbConnectionString))
			{
				dynamicDb.Select("dbo.Person", criteria: null);
			}
		}

		[TestMethod]
		public void Insert_OneRowWithAllColumnsProvided_RowIsInsertedAndReturned()
		{
			var records = new dynamic[]
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

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var insertedRecords = dynamicDb.Insert("dbo.Person", records);

				Assert.AreEqual(1, insertedRecords.Length);
				Assert.AreEqual(1, insertedRecords[0].Id);
				Assert.IsNotNull(insertedRecords[0].CreatedDate);
				Assert.IsNull(insertedRecords[0].UpdatedDate);
				DynamicDbTests.AssertPersonRecordMatches(records[0], insertedRecords[0]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(records);
			}
		}

		[TestMethod]
		public void Insert_OneRowWithAllRequiredColumnsProvided_RowIsInsertedAndReturned()
		{
			var records = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = (string)null,
					Age = 50,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var insertedRecords = dynamicDb.Insert("dbo.Person", records);

				Assert.AreEqual(1, insertedRecords.Length);
				Assert.AreEqual(1, insertedRecords[0].Id);
				Assert.IsNotNull(insertedRecords[0].CreatedDate);
				Assert.IsNull(insertedRecords[0].UpdatedDate);
				DynamicDbTests.AssertPersonRecordMatches(records[0], insertedRecords[0]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(records);
			}
		}

		[TestMethod]
		public void Insert_MultipleRows_RowsAreInsertedAndReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var insertedRecords = dynamicDb.Insert("dbo.Person", records);

				Assert.AreEqual(2, insertedRecords.Length);
				Assert.AreEqual(1, insertedRecords[0].Id);
				Assert.AreEqual(2, insertedRecords[1].Id);
				Assert.IsNotNull(insertedRecords[0].CreatedDate);
				Assert.IsNotNull(insertedRecords[1].CreatedDate);
				Assert.IsNull(insertedRecords[0].UpdatedDate);
				Assert.IsNull(insertedRecords[1].UpdatedDate);
				DynamicDbTests.AssertPersonRecordMatches(records[0], insertedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(records[1], insertedRecords[1]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(records);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void Insert_MultipleRowsWithDifferentColumnsProvidedOnEach_ExceptionIsThrown()
		{
			var records = new dynamic[]
			{
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
					Age = 40,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
				}
			};

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Insert("dbo.Person", records);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Insert_InvalidDataType_ExceptionIsThrown()
		{
			var records = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = "INVALID DATA TYPE",
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Insert("dbo.Person", records);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void Insert_TableDoesNotExist_ExceptionIsThrown()
		{
			var records = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = (string)null,
					Age = 50,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Insert("dbo.NonExistant", records);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Insert_RequiredColumnNotProvided_ExceptionIsThrown()
		{
			var records = new dynamic[]
			{
				new
				{
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 50,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Insert("dbo.Person", records);
			}
		}

		[TestMethod]
		public void Update_ZeroRowsInTable_ZeroRowsUpdatedAndReturned()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var updatedRecords = dynamicDb.Update("dbo.Person", values: new { Age = 30 }, criteria: null);

				Assert.AreEqual(0, updatedRecords.Length);
			}
		}

		[TestMethod]
		public void Update_OneColumnValueIsProvidedAndCriteriaIsNotProvided_RowsAreUpdatedAndReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			var recordsWithUpdatedValues = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 30,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 30,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var updatedRecords = dynamicDb.Update("dbo.Person", values: new { Age = 30 }, criteria: null);

				Assert.AreEqual(2, updatedRecords.Length);
				Assert.IsNotNull(updatedRecords[0].UpdatedDate);
				Assert.IsNotNull(updatedRecords[1].UpdatedDate);
				DynamicDbTests.AssertPersonRecordMatches(recordsWithUpdatedValues[0], updatedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(recordsWithUpdatedValues[1], updatedRecords[1]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(recordsWithUpdatedValues);
			}
		}

		[TestMethod]
		public void Update_OneColumnValueIsProvidedAndCriteriaIsProvided_FilteredRowIsUpdatedAndReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			var recordsWithUpdatedValues = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 30,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var updatedRecords = dynamicDb.Update("dbo.Person", values: new { Age = 30 }, criteria: new { FirstName = "John" });

				Assert.AreEqual(1, updatedRecords.Length);
				Assert.IsNotNull(updatedRecords[0].UpdatedDate);
				DynamicDbTests.AssertPersonRecordMatches(recordsWithUpdatedValues[0], updatedRecords[0]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(recordsWithUpdatedValues);
			}
		}

		[TestMethod]
		public void Update_MultipleColumnValuesAreProvidedAndCriteriaIsProvided_FilteredRowIsUpdatedAndReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};
			
			var recordsWithUpdatedValues = new dynamic[]
			{
				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 30,
					DateOfBirth = DateTime.Parse("2001-01-01"),
					Gender = Gender.Female
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var updatedRecords = dynamicDb.Update(
					"dbo.Person",
					values: new { Age = 30, DateOfBirth = DateTime.Parse("2001-01-01") },
					criteria: new { Id = 2 });

				Assert.AreEqual(1, updatedRecords.Length);
				Assert.IsNotNull(updatedRecords[0].UpdatedDate);
				DynamicDbTests.AssertPersonRecordMatches(recordsWithUpdatedValues[0], updatedRecords[0]);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(recordsWithUpdatedValues);
			}
		}

		[TestMethod]
		public void Update_MultipleColumnValuesAreProvidedAndMultipleCriteriaAreProvided_FilteredRowsAreUpdatedAndReturned()
		{
			var records = new dynamic[]
			{
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
				},

				new
				{
					FirstName = "Bruce",
					LastName = "Wayne",
					MiddleInitial = "X",
					Age = 30,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			var recordsWithUpdatedValues = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 30,
					DateOfBirth = DateTime.Parse("2001-01-01"),
					Gender = Gender.Male
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 30,
					DateOfBirth = DateTime.Parse("2001-01-01"),
					Gender = Gender.Female
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var updatedRecords = dynamicDb.Update(
					"dbo.Person",
					new { Age = 30, DateOfBirth = DateTime.Parse("2001-01-01") },
					new { FirstName = "John" }, new { FirstName = "Jane", LastName = "Doe" });

				Assert.AreEqual(2, updatedRecords.Length);
				Assert.IsNotNull(updatedRecords[0].UpdatedDate);
				Assert.IsNotNull(updatedRecords[1].UpdatedDate);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(recordsWithUpdatedValues);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Update_InvalidDataType_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Update("dbo.Person", new { Age = "Invalid" }, criteria: null);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Update_InvalidDataTypeInCriteria_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Update("dbo.Person", new { Age = 30 }, criteria: new { Age = "Invalid" });
			}
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void Update_TableDoesNotExist_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Update("dbo.NonExistant", new { Age = 30 }, criteria: null);
			}
		}

		[TestMethod]
		public void Delete_CriteriaNotProvidedAndZeroRowsInTable_ZeroRowsDeletedAndReturned()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var deletedRecords = dynamicDb.Delete("dbo.Person", criteria: null);

				Assert.AreEqual(0, deletedRecords.Length);
			}
		}

		[TestMethod]
		public void Delete_CriteriaIsNotProvided_RowsAreDeletedAndReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var deletedRecords = dynamicDb.Delete("dbo.Person", criteria: null);

				Assert.AreEqual(2, deletedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], deletedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(records[1], deletedRecords[1]);
				DynamicDbTests.AssertPersonRecordsDoNotExist(1, 2);
			}
		}

		[TestMethod]
		public void Delete_CriteriaIsProvided_FilteredRowIsDeletedAndReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var deletedRecords = dynamicDb.Delete("dbo.Person", criteria: new { FirstName = "John" });

				Assert.AreEqual(1, deletedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], deletedRecords[0]);
				DynamicDbTests.AssertPersonRecordsDoNotExist(1);
				DynamicDbTests.AssertPersonRecordsExist(2);
			}
		}

		[TestMethod]
		public void Delete_MultipleCriteriaAreProvided_FilteredRowIsDeletedAndReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var deletedRecords = dynamicDb.Delete("dbo.Person", criteria: new { Id = 2, FirstName = "Jane" });

				Assert.AreEqual(1, deletedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[1], deletedRecords[0]);
				DynamicDbTests.AssertPersonRecordsDoNotExist(2);
				DynamicDbTests.AssertPersonRecordsExist(1);
			}
		}

		[TestMethod]
		public void Delete_MultipleCriteriaAreProvided2_FilteredRowIsDeletedAndReturned()
		{
			var records = new dynamic[]
			{
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
				},

				new
				{
					FirstName = "Bruce",
					LastName = "Wayne",
					MiddleInitial = "X",
					Age = 30,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var deletedRecords = dynamicDb.Delete("dbo.Person",
					new { FirstName = "John" },
					new { FirstName = "Jane", LastName = "Doe" });

				Assert.AreEqual(2, deletedRecords.Length);
				DynamicDbTests.AssertPersonRecordsDoNotExist(1, 2);
				DynamicDbTests.AssertPersonRecordsExist(3);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Delete_InvalidDataType_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Delete("dbo.Person", new { Age = "Invalid" });
			}
		}

		[TestMethod]
		[ExpectedException(typeof(InvalidOperationException))]
		public void Delete_TableDoesNotExist_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Delete("dbo.NonExistant", criteria: null);
			}
		}
		
		[TestMethod]
		public void Query_SingleTableQueryViaCommandText_RowsAreReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Query("SELECT * FROM dbo.Person");

				Assert.AreEqual(2, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], selectedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(records[1], selectedRecords[1]);
			}
		}

		[TestMethod]
		public void Query_SingleTableQueryViaStoredProcedure_RowsAreReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Query("dbo.GetAllPeople", commandType: CommandType.StoredProcedure);

				Assert.AreEqual(2, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], selectedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(records[1], selectedRecords[1]);
			}
		}

		[TestMethod]
		public void Query_SingleTableQueryViaCommandTextWithParameter_FilteredRowIsReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Query("SELECT * FROM dbo.Person WHERE Age >= @MinimumAge", parameters: new { MinimumAge = 45 });

				Assert.AreEqual(1, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], selectedRecords[0]);
			}
		}

		[TestMethod]
		public void Query_SingleTableQueryViaStoredProcedureWithParameter_FilteredRowIsReturned()
		{
			var records = new dynamic[]
			{
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
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Query(
					"dbo.GetAllPeopleByMinimumAge",
					parameters: new { MinimumAge = 45 },
					commandType: CommandType.StoredProcedure);

				Assert.AreEqual(1, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(records[0], selectedRecords[0]);
			}
		}

		[TestMethod]
		public void Query_MultipleTablesQueryViaCommandText_RowsAreReturned()
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
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 40,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
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

			DynamicDbTests.InsertPersonRecords(personRecords);
			DynamicDbTests.InsertAddressRecords(addressRecords);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
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

				var selectedRecords = dynamicDb.Query(commandText);

				Assert.AreEqual(2, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(personRecords[0], selectedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(personRecords[1], selectedRecords[1]);
				DynamicDbTests.AssertAddressRecordMatches(addressRecords[0], selectedRecords[0]);
				Assert.IsNull(selectedRecords[1].AddressId);
				Assert.IsNull(selectedRecords[1].Street);
				Assert.IsNull(selectedRecords[1].Apt);
				Assert.IsNull(selectedRecords[1].City);
				Assert.IsNull(selectedRecords[1].State);
				Assert.IsNull(selectedRecords[1].ZipCode);
			}
		}

		[TestMethod]
		public void Query_MultipleTablesQueryViaStoredProcedure_RowsAreReturned()
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
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 40,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
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

			DynamicDbTests.InsertPersonRecords(personRecords);
			DynamicDbTests.InsertAddressRecords(addressRecords);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var selectedRecords = dynamicDb.Query("dbo.GetAllPeopleAndAddresses", commandType: CommandType.StoredProcedure);

				Assert.AreEqual(2, selectedRecords.Length);
				DynamicDbTests.AssertPersonRecordMatches(personRecords[0], selectedRecords[0]);
				DynamicDbTests.AssertPersonRecordMatches(personRecords[1], selectedRecords[1]);
				DynamicDbTests.AssertAddressRecordMatches(addressRecords[0], selectedRecords[0]);
				Assert.IsNull(selectedRecords[1].AddressId);
				Assert.IsNull(selectedRecords[1].Street);
				Assert.IsNull(selectedRecords[1].Apt);
				Assert.IsNull(selectedRecords[1].City);
				Assert.IsNull(selectedRecords[1].State);
				Assert.IsNull(selectedRecords[1].ZipCode);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Query_CommandTextReferencingTableThatDoesNotexist_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Query("SELECT * FROM dbo.NonExistant");
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Query_CommandTextWithInvalidDataProvidedForParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Query("SELECT * FROM dbo.Person WHERE Age >= @MinimumAge", parameters: new { MinimumAge = "Invalid" });
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Query_StoredProcedureWithInvalidDataProvidedForParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Query(
					"dbo.GetAllPeopleByMinimumAge",
					parameters: new { MinimumAge = "Invalid" },
					commandType: CommandType.StoredProcedure);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Query_CommandTextWithMissingParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Query("SELECT * FROM dbo.Person WHERE Age >= @MinimumAge");
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Query_StoredProcedureWithMissingParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Query("dbo.GetAllPeopleByMinimumAge", commandType: CommandType.StoredProcedure);
			}
		}

		[TestMethod]
		public void Execute_UpdateCommandTextWithParameter_ExpectedRecordsAreUpdated()
		{
			var records = new dynamic[]
			{
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
				}
			};

			var expectedRecords = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var affectedRows = dynamicDb.Execute("UPDATE dbo.Person SET Age = @Age", parameters: new { Age = 60 });

				Assert.AreEqual(2, affectedRows);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(expectedRecords);
			}
		}

		[TestMethod]
		public void Execute_UpdateCommandText_ExpectedRecordsAreUpdated()
		{
			var records = new dynamic[]
			{
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
				}
			};

			var expectedRecords = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var affectedRows = dynamicDb.Execute("UPDATE dbo.Person SET Age = 60");

				Assert.AreEqual(2, affectedRows);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(expectedRecords);
			}
		}

		[TestMethod]
		public void Execute_UpdateStoredProcedureWithParameter_ExpectedRecordsAreUpdated()
		{
			var records = new dynamic[]
			{
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
				}
			};

			var expectedRecords = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var affectedRows = dynamicDb.Execute(
					"dbo.UpdateAllAges",
					parameters: new { Age = 60 },
					commandType: CommandType.StoredProcedure);

				Assert.AreEqual(2, affectedRows);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(expectedRecords);
			}
		}

		[TestMethod]
		public void Execute_UpdateStoredProcedure_ExpectedRecordsAreUpdated()
		{
			var records = new dynamic[]
			{
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
				}
			};

			var expectedRecords = new dynamic[]
			{
				new
				{
					FirstName = "John",
					LastName = "Doe",
					MiddleInitial = "A",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Male
				},

				new
				{
					FirstName = "Jane",
					LastName = "Doe",
					MiddleInitial = "M",
					Age = 60,
					DateOfBirth = DateTime.Parse("2000-01-01"),
					Gender = Gender.Female
				}
			};

			DynamicDbTests.InsertPersonRecords(records);

			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				var affectedRows = dynamicDb.Execute("dbo.UpdateAllAgesTo60", commandType: CommandType.StoredProcedure);

				Assert.AreEqual(2, affectedRows);
				DynamicDbTests.AssertPersonRecordsExistAndMatch(expectedRecords);
			}
		}
		
		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Execute_CommandTextReferencingTableThatDoesNotexist_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Execute("DELETE dbo.NonExistant");
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Execute_CommandTextWithInvalidDataProvidedForParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Execute("DELETE dbo.Person WHERE Age >= @MinimumAge", parameters: new { MinimumAge = "Invalid" });
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Execute_StoredProcedureWithInvalidDataProvidedForParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Query(
					"dbo.UpdateAllAges",
					parameters: new { Age = "Invalid" },
					commandType: CommandType.StoredProcedure);
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Execute_CommandTextWithMissingParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Execute("DELETE dbo.Person WHERE Age >= @MinimumAge");
			}
		}

		[TestMethod]
		[ExpectedException(typeof(SqlException))]
		public void Execute_StoredProcedureWithMissingParameter_ExceptionIsThrown()
		{
			using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
			{
				dynamicDb.Query("dbo.UpdateAllAges", commandType: CommandType.StoredProcedure);
			}
		}

		[TestMethod]
		public void Select_ParallelCallsWithDifferentDynamicDbObject_NoExceptionIsThrown()
		{
			Parallel.For(0, 10, i =>
			{
				using (var dynamicDb = new DynamicDb(DynamicDbTests.DbConnectionString))
				{
					var selectedRecords = dynamicDb.Select("dbo.Person", criteria: null);
				}
			});
		}

		internal static void InsertPersonRecords(dynamic[] records)
		{
			var commandText = new StringBuilder("INSERT dbo.Person (FirstName, LastName, MiddleInitial, Age, DateOfBirth, Gender) VALUES ");

			for (int i = 0; i < records.Length; i++)
			{
				if (i > 0)
				{
					commandText.Append(", ");
				}

				commandText.Append($"('{records[i].FirstName}', '{records[i].LastName}', ")
					.Append(records[i].MiddleInitial != null ? $"'{records[i].MiddleInitial}', " : "NULL, ")
					.Append($"{records[i].Age}, '{records[i].DateOfBirth}', {(byte)records[i].Gender})");
			}

			using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
			{
				connection.Open();
				
				using (var command = new SqlCommand(commandText.ToString(), connection))
				{
					command.ExecuteNonQuery();
				}
			}
		}

		internal static void InsertAddressRecords(dynamic[] records)
		{
			var commandText = new StringBuilder("INSERT dbo.Address (PersonId, Street, Apt, City, State, ZipCode) VALUES ");

			for (int i = 0; i < records.Length; i++)
			{
				if (i > 0)
				{
					commandText.Append(", ");
				}

				commandText.Append($"({records[i].PersonId}, '{records[i].Street}', ")
					.Append(records[i].Apt != null ? $"'{records[i].Apt}', " : "NULL, ")
					.Append($"'{records[i].City}', '{records[i].State}', '{records[i].ZipCode}')");
			}

			using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
			{
				connection.Open();

				using (var command = new SqlCommand(commandText.ToString(), connection))
				{
					command.ExecuteNonQuery();
				}
			}
		}

		internal static void AssertPersonRecordsExistAndMatch(dynamic[] records)
		{
			for (int i = 0; i < records.Length; i++)
			{
				var commandText = new StringBuilder($"SELECT 1 FROM dbo.Person WHERE FirstName = '{records[i].FirstName}' AND LastName = '{records[i].LastName}' AND ")
					.Append(records[i].MiddleInitial != null ? $"MiddleInitial = '{records[i].MiddleInitial}' AND " : "MiddleInitial IS NULL AND ")
					.Append($"Age = {records[i].Age} AND DateOfBirth = '{records[i].DateOfBirth}' AND Gender = {(byte)records[i].Gender}");

				using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
				{
					connection.Open();

					using (var command = new SqlCommand(commandText.ToString(), connection))
					using (var reader = command.ExecuteReader())
					{
						var recordCount = 0;

						while (reader.Read())
						{
							recordCount++;
						}

						Assert.AreEqual(1, recordCount);
					}
				}
			}
		}

		internal static void AssertPersonRecordsExist(params int[] ids)
		{
			var commandText = new StringBuilder("SELECT 1 FROM dbo.Person WHERE Id IN (");

			for (int i = 0; i < ids.Length; i++)
			{
				if (i > 0)
				{
					commandText.Append(", ");
				}

				commandText.Append(ids[i]);
			}

			commandText.Append(")");

			using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
			{
				connection.Open();

				using (var command = new SqlCommand(commandText.ToString(), connection))
				using (var reader = command.ExecuteReader())
				{
					var recordCount = 0;

					while (reader.Read())
					{
						recordCount++;
					}

					Assert.AreEqual(ids.Length, recordCount);
				}
			}
		}

		internal static void AssertPersonRecordsDoNotExist(params int[] ids)
		{
			var commandText = new StringBuilder("SELECT 1 FROM dbo.Person WHERE Id IN (");

			for (int i = 0; i < ids.Length; i++)
			{
				if (i > 0)
				{
					commandText.Append(", ");
				}

				commandText.Append(ids[i]);
			}

			commandText.Append(")");

			using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
			{
				connection.Open();

				using (var command = new SqlCommand(commandText.ToString(), connection))
				using (var reader = command.ExecuteReader())
				{
					var recordCount = 0;

					while (reader.Read())
					{
						recordCount++;
					}

					Assert.AreEqual(0, recordCount);
				}
			}
		}

		internal static void AssertPersonRecordMatches(dynamic expected, dynamic actual)
		{
			Assert.AreEqual(expected.FirstName, actual.FirstName);
			Assert.AreEqual(expected.LastName, actual.LastName);
			Assert.AreEqual(expected.MiddleInitial, actual.MiddleInitial);
			Assert.AreEqual(expected.Age, actual.Age);
			Assert.AreEqual(expected.DateOfBirth, actual.DateOfBirth);
			Assert.AreEqual(expected.Gender, (Gender)actual.Gender);
		}

		internal static void AssertAddressRecordMatches(dynamic expected, dynamic actual)
		{
			Assert.AreEqual(expected.Street, actual.Street);
			Assert.AreEqual(expected.Apt, actual.Apt);
			Assert.AreEqual(expected.City, actual.City);
			Assert.AreEqual(expected.State, actual.State);
			Assert.AreEqual(expected.ZipCode, actual.ZipCode);
		}

		internal static void AssertAddressRecordsDoNotExist(params int[] ids)
		{
			var commandText = new StringBuilder("SELECT 1 FROM dbo.Address WHERE Id IN (");

			for (int i = 0; i < ids.Length; i++)
			{
				if (i > 0)
				{
					commandText.Append(", ");
				}

				commandText.Append(ids[i]);
			}

			commandText.Append(")");

			using (var connection = new SqlConnection(DynamicDbTests.DbConnectionString))
			{
				connection.Open();

				using (var command = new SqlCommand(commandText.ToString(), connection))
				using (var reader = command.ExecuteReader())
				{
					var recordCount = 0;

					while (reader.Read())
					{
						recordCount++;
					}

					Assert.AreEqual(0, recordCount);
				}
			}
		}

		internal enum Gender
		{
			Undefined,
			Male,
			Female
		}
	}
}
