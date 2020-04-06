using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace DynamicDb
{
	public class TableMetadataProvider : ITableMetadataProvider
	{
		protected static Dictionary<string, Cache> Caches = new Dictionary<string, Cache>();

		public TableMetadataProvider(SqlConnection dbConnection)
		{
			this.DbConnection = dbConnection;

			if (!TableMetadataProvider.Caches.TryGetValue(dbConnection.ConnectionString, out var cache))
			{
				cache = new Cache();

				TableMetadataProvider.Caches.Add(dbConnection.ConnectionString, cache);
			}

			this.CachedData = cache;
		}

		protected SqlConnection DbConnection { get; private set; }
		protected Cache CachedData { get; private set; }

		public IEnumerable<SqlColumn> GetColumnDefinitions(string table)
		{
			if (!this.CachedData.ColumnDefinitions.TryGetValue(table, out var columnDefinitions))
			{
				columnDefinitions = new List<SqlColumn>();

				this.CachedData.ColumnDefinitions.Add(table, columnDefinitions);

				var schemaMatch = Regex.Match(table, @"\[*(\w+)\]*\.\[*(\w+)\]*");
				var schema = schemaMatch.Success ? schemaMatch.Groups[1].Value : null;
				var tableWithoutSchema = schemaMatch.Success ? schemaMatch.Groups[2].Value : null;

				var commandText = @"
					SELECT c.COLUMN_NAME,
						c.DATA_TYPE,
						c.CHARACTER_MAXIMUM_LENGTH,
						c.NUMERIC_PRECISION,
						c.NUMERIC_SCALE,
						CAST(CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS BIT),
						IsPrimaryKey = CAST(MAX(CASE WHEN OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 THEN 1 ELSE 0 END) AS BIT)
					FROM INFORMATION_SCHEMA.COLUMNS c
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON c.TABLE_NAME = kcu.TABLE_NAME
						AND c.COLUMN_NAME = kcu.COLUMN_NAME
					WHERE c.TABLE_SCHEMA = ISNULL(@Schema, SCHEMA_NAME())
						AND c.TABLE_NAME = @Table
					GROUP BY c.COLUMN_NAME,
						c.DATA_TYPE,
						c.CHARACTER_MAXIMUM_LENGTH,
						c.NUMERIC_PRECISION,
						c.NUMERIC_SCALE,
						c.IS_NULLABLE,
						c.ORDINAL_POSITION
					ORDER BY c.ORDINAL_POSITION";

				using (var command = new SqlCommand(commandText, this.DbConnection))
				{
					command.Parameters.AddWithValue("Schema", schema ?? (object)DBNull.Value);
					command.Parameters.AddWithValue("Table", tableWithoutSchema);

					using (var reader = command.ExecuteReader())
					{
						while (reader.Read())
						{
							var columnDefinition = new SqlColumn()
							{
								Name = reader.GetString(0),
								DataType = reader.GetString(1),
								MaximumLength = !reader.IsDBNull(2) ? reader.GetInt32(2) : (int?)null,
								Precision = !reader.IsDBNull(3) ? reader.GetByte(3) : (byte?)null,
								Scale = !reader.IsDBNull(4) ? reader.GetInt32(4) : (int?)null,
								IsNullable = reader.GetBoolean(5),
								IsPrimaryKey = reader.GetBoolean(6)
							};

							((List<SqlColumn>)columnDefinitions).Add(columnDefinition);
						}
					}
				}
			}

			if (columnDefinitions.Count() == 0)
			{
				throw new InvalidOperationException($"Error retrieving schema information for table '{table}'.  The table may not exist or the user may not have permissions.");
			}

			return columnDefinitions;
		}

		protected class Cache
		{
			public Dictionary<string, IEnumerable<SqlColumn>> ColumnDefinitions { get; set; } = new Dictionary<string, IEnumerable<SqlColumn>>();
		}
	}
}
