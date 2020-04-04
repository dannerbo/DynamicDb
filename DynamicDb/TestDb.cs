using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace DynamicDb
{
	public class TestDb : DynamicDb
	{
		private Stack<RecordSet> insertedRecordsCache = new Stack<RecordSet>();
		private bool rollbackOnDispose;
		private SqlTransaction transaction;

		public TestDb(string connectionString, bool rollbackOnDispose = false)
			: base(connectionString)
		{
			this.rollbackOnDispose = rollbackOnDispose;
		}

		public TestDb(SqlConnection connection, bool rollbackOnDispose = false)
			: base(connection)
		{
			this.rollbackOnDispose = rollbackOnDispose;
		}

		public dynamic[] Insert(string table, bool deleteOnDispose, params object[] records)
		{
			Throw.If(
				() => deleteOnDispose && this.rollbackOnDispose,
				() => new InvalidOperationException($"Argument '{nameof(deleteOnDispose)}' is expected to be 'false' when constructor argument '{nameof(this.rollbackOnDispose)}' is 'true'."));

			var insertedRecords = this.Insert(table, records);

			if (deleteOnDispose)
			{
				this.insertedRecordsCache.Push(new RecordSet(table, insertedRecords));
			}

			return insertedRecords;
		}

		protected override DbCommandGenerator CreateCommandGenerator()
		{
			if (this.rollbackOnDispose)
			{
				this.transaction = this.Connection.BeginTransaction();

				return new DbCommandGenerator(this.TableMetadataProvider, new DbCommandFactory(this.transaction));
			}

			return base.CreateCommandGenerator();
		}

		protected virtual void DeleteInsertedRecords()
		{
			while (this.insertedRecordsCache.Count > 0)
			{
				var recordSet = this.insertedRecordsCache.Pop();
				
				using (var command = this.CommandGenerator.GenerateDeleteByPrimaryKeys(recordSet.Table, recordSet.Records))
				{
					command.Connection = this.Connection;

					command.ExecuteNonQuery();
				}
			}
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				if (this.rollbackOnDispose && this.transaction != null)
				{
					this.transaction.Rollback();
					this.transaction.Dispose();

					this.transaction = null;
				}

				if (this.insertedRecordsCache.Count > 0)
				{
					this.DeleteInsertedRecords();
				}
			}

			base.Dispose(disposing);
		}

		private class RecordSet
		{
			public RecordSet(string table, object[] records)
			{
				this.Table = table;
				this.Records = records;
			}

			public string Table { get; private set; }
			public object[] Records { get; private set; }
		}
	}
}
