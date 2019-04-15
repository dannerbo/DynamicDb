using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace DynamicDb
{
	public class DbCommandGenerator
	{
		protected static Dictionary<string, Cache> Caches = new Dictionary<string, Cache>();
		
		public DbCommandGenerator(SqlConnection dbConnection)
		{
			this.DbConnection = dbConnection;

			if (!DbCommandGenerator.Caches.TryGetValue(dbConnection.ConnectionString, out var cache))
			{
				cache = new Cache();

				DbCommandGenerator.Caches.Add(dbConnection.ConnectionString, cache);
			}

			this.CachedData = cache;
		}

		protected SqlConnection DbConnection { get; private set; }
		protected Cache CachedData { get; private set; }

		public SqlCommand GenerateInsert(string table, params object[] records)
		{
			ExceptionHelper.ThrowIf(() => table == null, () => new ArgumentNullException(nameof(table)));
			ExceptionHelper.ThrowIf(() => records == null, () => new ArgumentNullException(nameof(records)));
			ExceptionHelper.ThrowIf(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));
			ExceptionHelper.ThrowIf(() => records.Length == 0, () => new ArgumentException("There were no records provided.", nameof(records)));

			this.GenerateOutputTempTableArtifacts(
				table,
				false,
				out var declareTempTable,
				out var outputIntoTempTable,
				out var selectFromTempTable,
				out var joinTableToTempTable);

			var properties = records.First().GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
			var columnsDelimited = String.Join(", ", properties.Select(p => p.Name));
			var insertValuesDelimited = this.GenerateDelimitedInsertValues(records, properties, out var parameters);

			var commandTextStringBuilder = new StringBuilder()
				.AppendLine(declareTempTable)
				.AppendLine()
				.AppendLine($"INSERT {table} ({columnsDelimited})")
				.AppendLine(outputIntoTempTable)
				.AppendLine($"VALUES {insertValuesDelimited}")
				.AppendLine()
				.AppendLine(selectFromTempTable);

			if (joinTableToTempTable != null)
			{
				commandTextStringBuilder.AppendLine(joinTableToTempTable);
			}

			var command = new SqlCommand(commandTextStringBuilder.ToString());

			if (parameters?.Length > 0)
			{
				command.Parameters.AddRange(parameters);
			}

			return command;
		}

		public SqlCommand GenerateSelect(string table, params object[] criteria)
		{
			ExceptionHelper.ThrowIf(() => table == null, () => new ArgumentNullException(nameof(table)));
			ExceptionHelper.ThrowIf(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

			var commandTextStringBuilder = new StringBuilder()
				.AppendLine("SELECT *")
				.AppendLine($"FROM {table}");

			SqlParameter[] parameters = null;

			if (criteria?.Length > 0)
			{
				var whereConditions = this.GenerateWhereConditions(criteria, out parameters);

				if (!String.IsNullOrEmpty(whereConditions))
				{
					commandTextStringBuilder.AppendLine($"WHERE {whereConditions}");
				}
			}
			
			var command = new SqlCommand(commandTextStringBuilder.ToString());

			if (parameters?.Length > 0)
			{
				command.Parameters.AddRange(parameters);
			}

			return command;
		}

		public SqlCommand GenerateDelete(string table, params object[] criteria)
		{
			ExceptionHelper.ThrowIf(() => table == null, () => new ArgumentNullException(nameof(table)));
			ExceptionHelper.ThrowIf(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

			this.GenerateOutputTempTableArtifacts(
				table,
				true,
				out var declareTempTable,
				out var outputIntoTempTable,
				out var selectFromTempTable,
				out var joinTableToTempTable);

			var commandTextStringBuilder = new StringBuilder()
				.AppendLine(declareTempTable)
				.AppendLine()
				.AppendLine($"DELETE {table}")
				.AppendLine(outputIntoTempTable);

			SqlParameter[] parameters = null;

			if (criteria?.Length > 0)
			{
				var whereConditions = this.GenerateWhereConditions(criteria, out parameters);

				commandTextStringBuilder.AppendLine($"WHERE {whereConditions}");
			}

			commandTextStringBuilder
				.AppendLine()
				.AppendLine(selectFromTempTable);

			if (joinTableToTempTable != null)
			{
				commandTextStringBuilder.AppendLine(joinTableToTempTable);
			}

			var command = new SqlCommand(commandTextStringBuilder.ToString());

			if (parameters?.Length > 0)
			{
				command.Parameters.AddRange(parameters);
			}

			return command;
		}

		public SqlCommand GenerateUpdate(string table, object values, params object[] criteria)
		{
			ExceptionHelper.ThrowIf(() => table == null, () => new ArgumentNullException(nameof(table)));
			ExceptionHelper.ThrowIf(() => values == null, () => new ArgumentNullException(nameof(values)));
			ExceptionHelper.ThrowIf(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

			this.GenerateOutputTempTableArtifacts(
				table,
				false,
				out var declareTempTable,
				out var outputIntoTempTable,
				out var selectFromTempTable,
				out var joinTableToTempTable);

			var setValues = this.GenerateSetValues(values, out var setParameters);

			var commandTextStringBuilder = new StringBuilder()
				.AppendLine(declareTempTable)
				.AppendLine()
				.AppendLine($"UPDATE {table}")
				.AppendLine($"SET {setValues}")
				.AppendLine(outputIntoTempTable);

			SqlParameter[] whereParameters = null;

			if (criteria?.Length > 0)
			{
				var whereConditions = this.GenerateWhereConditions(criteria, out whereParameters);

				if (!String.IsNullOrEmpty(whereConditions))
				{
					commandTextStringBuilder.AppendLine($"WHERE {whereConditions}");
				}
			}

			commandTextStringBuilder
				.AppendLine()
				.AppendLine(selectFromTempTable);

			if (joinTableToTempTable != null)
			{
				commandTextStringBuilder.AppendLine(joinTableToTempTable);
			}

			var command = new SqlCommand(commandTextStringBuilder.ToString());

			command.Parameters.AddRange(setParameters);

			if (whereParameters?.Length > 0)
			{
				command.Parameters.AddRange(whereParameters);
			}

			return command;
		}

		public SqlCommand Generate(string commandText, object parameters, CommandType commandType)
		{
			ExceptionHelper.ThrowIf(() => commandText == null, () => new ArgumentNullException(nameof(commandText)));
			ExceptionHelper.ThrowIf(() => commandText.Length == 0, () => new ArgumentException("Command text was not provided.", nameof(commandText)));

			List<IDbDataParameter> parameterList = null;

			if (parameters != null)
			{
				parameterList = new List<IDbDataParameter>();

				var properties = parameters.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

				foreach (var property in properties)
				{
					var propertyValue = property.GetValue(parameters);

					parameterList.Add(new SqlParameter(property.Name, propertyValue ?? DBNull.Value));
				}
			}

			var command = new SqlCommand(commandText);

			command.CommandType = commandType;

			if (parameterList != null)
			{
				command.Parameters.AddRange(parameterList.ToArray());
			}

			return command;
		}

		protected string GenerateDelimitedColumnDefinitions(IEnumerable<SqlColumn> columns)
		{
			var delimitedColumnDefinitions = new StringBuilder();

			foreach (var column in columns)
			{
				if (delimitedColumnDefinitions.Length > 0)
				{
					delimitedColumnDefinitions.Append(", ");
				}

				var dataType = column.DataType.ToUpper();

				switch (dataType)
				{
					case "BIGINT":
					case "BIT":
					case "DATE":
					case "DATETIME":
					case "IMAGE":
					case "INT":
					case "MONEY":
					case "NTEXT":
					case "ROWVERSION":
					case "SMALLDATETIME":
					case "SMALLINT":
					case "SMALLMONEY":
					case "SQL_VARIANT":
					case "TEXT":
					case "TINYINT":
					case "UNIQUEIDENTIFIER":
					case "XML":
					{
						delimitedColumnDefinitions.Append($"{column.Name} {dataType}");

						break;
					}

					case "DECIMAL":
					case "NUMERIC":
					{
						delimitedColumnDefinitions.Append($"{column.Name} {dataType}({column.Precision}, {column.Scale})");

						break;
					}

					case "DATETIME2":
					case "DATETIMEOFFSET":
					case "FLOAT":
					case "REAL":
					{
						delimitedColumnDefinitions.Append($"{column.Name} {dataType}({column.Precision})");

						break;
					}

					case "TIME":
					{
						delimitedColumnDefinitions.Append($"{column.Name} {dataType}({column.Scale})");

						break;
					}

					case "BINARY":
					case "CHAR":
					case "NCHAR":
					case "NVARCHAR":
					case "VARBINARY":
					case "VARCHAR":
					{
						var columnMaximumLength = column.MaximumLength == -1 ? "MAX" : column.MaximumLength.ToString();

						delimitedColumnDefinitions.Append($"{column.Name} {dataType}({columnMaximumLength})");

						break;
					}

					default:
					{
						throw new NotSupportedException($"Column type '{dataType}' not supported.");
					}
				}
			}

			return delimitedColumnDefinitions.ToString();
		}

		protected string GenerateDelimitedInsertValues(object[] records, PropertyInfo[] properties, out SqlParameter[] parameters)
		{
			var parameterList = new List<SqlParameter>();
			var insertValuesStringBuilder = new StringBuilder();

			for (int i = 0; i < records.Length; i++)
			{
				var record = records[i];

				if (insertValuesStringBuilder.Length > 0)
				{
					insertValuesStringBuilder.Append(", ");
				}

				insertValuesStringBuilder.Append("(");

				for (int j = 0; j < properties.Length; j++)
				{
					var property = properties[j];

					if (j > 0)
					{
						insertValuesStringBuilder.Append(", ");
					}

					object propertyValue;

					try
					{
						propertyValue = property.GetValue(record);
					}
					catch (TargetException targetException)
					{
						throw new InvalidOperationException("Error getting column value from record.  Make sure all records have identical columns.", targetException);
					}

					if (propertyValue != null)
					{
						var parameterName = $"{property.Name}_{i}";

						insertValuesStringBuilder.Append($"@{parameterName}");

						parameterList.Add(new SqlParameter(parameterName, propertyValue));
					}
					else
					{
						insertValuesStringBuilder.Append("NULL");
					}
				}

				insertValuesStringBuilder.Append(")");
			}

			parameters = parameterList.ToArray();

			return insertValuesStringBuilder.ToString();
		}

		protected void GenerateOutputTempTableArtifacts(
			string table,
			bool isForDelete,
			out string declareTempTable,
			out string outputIntoTempTable,
			out string selectFromTempTable,
			out string joinTableToTempTable)
		{
			joinTableToTempTable = null;

			var columnDefinitions = this.GetColumnDefinitions(table);
			var primaryKeyColumnDefinitions = columnDefinitions.Where(x => x.IsPrimaryKey);
			var idColumnDefinitions = !isForDelete && primaryKeyColumnDefinitions.Count() > 0 ? primaryKeyColumnDefinitions : columnDefinitions;
			var columnDefinitionsDelimited = this.GenerateDelimitedColumnDefinitions(idColumnDefinitions);

			declareTempTable = $"DECLARE @Record TABLE ({columnDefinitionsDelimited})";

			var outputValues = isForDelete ? "DELETED" : "INSERTED";
			var outputColumnsDelimited = String.Join(", ", idColumnDefinitions.Select(c => $"{outputValues}.{c.Name}"));

			outputIntoTempTable = $"OUTPUT {outputColumnsDelimited} INTO @Record";

			var selectFromTempTableStringBuilder = new StringBuilder();

			if (!isForDelete && primaryKeyColumnDefinitions.Count() > 0)
			{
				selectFromTempTableStringBuilder.AppendLine($"SELECT {table}.*");
			}
			else
			{
				selectFromTempTableStringBuilder.AppendLine($"SELECT *");
			}

			selectFromTempTableStringBuilder.Append("FROM @Record");

			selectFromTempTable = selectFromTempTableStringBuilder.ToString();

			if (!isForDelete && primaryKeyColumnDefinitions.Count() > 0)
			{
				var joinConditionsDelimited = this.GenerateDelimitedJoinConditions($"{table}", "[@Record]", idColumnDefinitions);

				joinTableToTempTable = $"JOIN {table} ON {joinConditionsDelimited}";
			}
		}

		protected string GenerateDelimitedJoinConditions(string table, string otherTable, IEnumerable<SqlColumn> columns)
		{
			var joinConditionsStringBuilder = new StringBuilder();

			foreach (var column in columns)
			{
				if (joinConditionsStringBuilder.Length > 0)
				{
					joinConditionsStringBuilder.Append(" AND ");
				}

				joinConditionsStringBuilder.Append($"{otherTable}.{column.Name} = {table}.{column.Name}");
			}

			return joinConditionsStringBuilder.ToString();
		}

		protected string GenerateWhereConditions(object[] criteria, out SqlParameter[] parameters)
		{
			var parameterList = new List<SqlParameter>();
			var whereConditionsStringBuilder = new StringBuilder();

			for (int i = 0; i < criteria.Length; i++)
			{
				var item = criteria[i];
				var properties = item.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
				
				whereConditionsStringBuilder.Append(i == 0 ? "(" : " OR (");

				for (int j = 0; j < properties.Length; j++)
				{
					var property = properties[j];
					var propertyValue = property.GetValue(item);

					if (j > 0)
					{
						whereConditionsStringBuilder.Append(" AND ");
					}

					if (propertyValue != null)
					{
						var parameterName = $"{property.Name}_{i}";

						whereConditionsStringBuilder.Append($"{property.Name} = @{parameterName}");

						parameterList.Add(new SqlParameter(parameterName, propertyValue));
					}
					else
					{
						whereConditionsStringBuilder.Append($"{property.Name} IS NULL");
					}
				}

				whereConditionsStringBuilder.Append(")");
			}

			parameters = parameterList.ToArray();

			return whereConditionsStringBuilder.ToString();
		}
		
		protected string GenerateSetValues(object values, out SqlParameter[] parameters)
		{
			var parameterList = new List<SqlParameter>();
			var setValuesStringBuilder = new StringBuilder();
			var properties = values.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

			foreach (var property in properties)
			{
				if (setValuesStringBuilder.Length > 0)
				{
					setValuesStringBuilder.Append(", ");
				}

				var propertyValue = property.GetValue(values);

				if (propertyValue != null)
				{
					var parameterName = $"set_{property.Name}";

					setValuesStringBuilder.Append($"{property.Name} = @{parameterName}");

					parameterList.Add(new SqlParameter(parameterName, propertyValue ?? DBNull.Value));
				}
				else
				{
					setValuesStringBuilder.Append($"{property.Name} = NULL");
				}
			}

			parameters = parameterList.ToArray();

			return setValuesStringBuilder.ToString();
		}

		protected IEnumerable<SqlColumn> GetColumnDefinitions(string table)
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

		protected class SqlColumn
		{
			public string Name { get; set; }
			public string DataType { get; set; }
			public int? MaximumLength { get; set; }
			public byte? Precision { get; set; }
			public int? Scale { get; set; }
			public bool IsNullable { get; set; }
			public bool IsPrimaryKey { get; set; }
		}
	}
}
