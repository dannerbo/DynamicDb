using System.Collections.Generic;
using System.Data.SqlClient;
using System.Transactions;

namespace DynamicDb
{
	public class TestDb : DynamicDb
	{
		private Stack<RecordSet> insertedRecordsCache = new Stack<RecordSet>();
		private TransactionScope transactionScope;

		public TestDb(string connectionString, bool rollbackWithTransactionScope = false)
			: base(connectionString)
		{
			if (rollbackWithTransactionScope)
			{
				this.transactionScope = new TransactionScope();
			}
		}

		public TestDb(SqlConnection connection)
			: base(connection)
		{
		}

		public dynamic[] Insert(string table, bool deleteOnDispose, params object[] records)
		{
			var insertedRecords = this.Insert(table, records);

			if (deleteOnDispose)
			{
				this.insertedRecordsCache.Push(new RecordSet(table, insertedRecords));
			}

			return insertedRecords;
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
				if (this.insertedRecordsCache.Count > 0)
				{
					this.DeleteInsertedRecords();
				}

				if (this.transactionScope != null)
				{
					this.transactionScope.Dispose();

					this.transactionScope = null;
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
