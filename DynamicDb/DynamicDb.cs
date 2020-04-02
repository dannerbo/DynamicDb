using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace DynamicDb
{
	public class DynamicDb : IDisposable
	{
		private SqlConnection connection;
		private DbCommandGenerator commandGenerator;
		private TableMetadataProvider tableMetadataProvider;

		public DynamicDb(string connectionString)
		{
			this.ConnectionString = connectionString;
		}

		public DynamicDb(SqlConnection connection)
		{
			this.connection = connection;
		}

		protected string ConnectionString { get; private set; }
		protected SqlConnection Connection => this.GetOrCreateConnection();
		protected TableMetadataProvider TableMetadataProvider => this.GetOrCreateTableMetadataProvider();
		protected DbCommandGenerator CommandGenerator => this.GetOrCreateCommandGenerator();

		public dynamic[] Insert(string table, params object[] records)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => records == null, () => new ArgumentNullException(nameof(records)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));
			Throw.If(() => records.Length == 0, () => new ArgumentException("There were no records provided.", nameof(records)));

			using (var command = this.CommandGenerator.GenerateInsert(table, records))
			{
				return this.ExecuteReader(command, (recordFactory, reader) => recordFactory.Create(reader, table));
			}
		}

		public dynamic[] Select(string table, params object[] criteria)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

			using (var command = this.CommandGenerator.GenerateSelect(table, criteria))
			{
				return this.ExecuteReader(command, (recordFactory, reader) => recordFactory.Create(reader, table));
			}
		}
		
		public dynamic[] Delete(string table, params object[] criteria)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

			using (var command = this.CommandGenerator.GenerateDelete(table, criteria))
			{
				return this.ExecuteReader(command, (recordFactory, reader) => recordFactory.Create(reader, table));
			}
		}

		public dynamic[] Update(string table, object values, params object[] criteria)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => values == null, () => new ArgumentNullException(nameof(values)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

			using (var command = this.CommandGenerator.GenerateUpdate(table, values, criteria))
			{
				return this.ExecuteReader(command, (recordFactory, reader) => recordFactory.Create(reader, table));
			}
		}

		public int Execute(string commandText, object parameters = null, CommandType commandType = CommandType.Text)
		{
			Throw.If(() => commandText == null, () => new ArgumentNullException(nameof(commandText)));
			Throw.If(() => commandText.Length == 0, () => new ArgumentException("Command text was not provided.", nameof(commandText)));
			
			using (var command = this.CommandGenerator.Generate(commandText, parameters, commandType))
			{
				command.Connection = this.GetOrCreateConnection();

				return command.ExecuteNonQuery();
			}
		}

		public dynamic[] Query(string commandText, object parameters = null, CommandType commandType = CommandType.Text)
		{
			Throw.If(() => commandText == null, () => new ArgumentNullException(nameof(commandText)));
			Throw.If(() => commandText.Length == 0, () => new ArgumentException("Command text was not provided.", nameof(commandText)));

			using (var command = this.CommandGenerator.Generate(commandText, parameters, commandType))
			{
				return this.ExecuteReader(command, (recordFactory, reader) => recordFactory.Create(reader, command));
			}
		}

		protected dynamic[] ExecuteReader(IDbCommand command, Func<RecordFactory, IDataReader, object> createRecord)
		{
			var records = new List<dynamic>();
			var recordFactory = RecordFactory.Create(this.ConnectionString);

			command.Connection = this.Connection;

			using (var reader = command.ExecuteReader())
			{
				while (reader.Read())
				{
					records.Add(createRecord(recordFactory, reader));
				}
			}

			return records.ToArray();
		}

		protected virtual SqlConnection GetOrCreateConnection()
		{
			if (this.connection == null)
			{
				this.connection = new SqlConnection(this.ConnectionString);
			}

			if (this.connection.State != ConnectionState.Open)
			{
				this.connection.Open();
			}

			return this.connection;
		}

		protected virtual TableMetadataProvider GetOrCreateTableMetadataProvider()
		{
			return this.tableMetadataProvider ?? (this.tableMetadataProvider = new TableMetadataProvider(this.Connection));
		}

		protected virtual DbCommandGenerator GetOrCreateCommandGenerator()
		{
			return this.commandGenerator ?? (this.commandGenerator = new DbCommandGenerator(this.TableMetadataProvider));
		}

		public void Dispose()
		{
			this.Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				if (this.ConnectionString != null && this.connection != null)
				{
					this.connection.Dispose();
					this.connection = null;
				}
			}
		}
	}
}
