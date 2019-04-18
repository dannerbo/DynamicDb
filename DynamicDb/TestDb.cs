using System.Collections.Generic;
using System.Data.SqlClient;

namespace DynamicDb
{
	public class TestDb : DynamicDb
	{
		private Stack<RecordSet> insertedRecordsCache = new Stack<RecordSet>();
		
		public TestDb(string connectionString)
			: base(connectionString)
		{
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
