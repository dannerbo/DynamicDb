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

		public IDbCommand GenerateInsert(string table, params object[] records)
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
			var insertValuesDelimited = this.GenerateDelimitedInsertValues(records, properties);

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

			return new SqlCommand(commandTextStringBuilder.ToString());
		}

		public IDbCommand GenerateSelect(string table, object criteria)
		{
			ExceptionHelper.ThrowIf(() => table == null, () => new ArgumentNullException(nameof(table)));
			ExceptionHelper.ThrowIf(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

			var commandTextStringBuilder = new StringBuilder()
				.AppendLine("SELECT *")
				.AppendLine($"FROM {table}");

			IDbDataParameter[] parameters = null;

			if (criteria != null)
			{
				var whereConditions = this.GetWhereConditions(criteria, out parameters);

				if (!String.IsNullOrEmpty(whereConditions))
				{
					commandTextStringBuilder.AppendLine($"WHERE {whereConditions}");
				}
			}

			var command = new SqlCommand(commandTextStringBuilder.ToString());

			if (parameters != null)
			{
				command.Parameters.AddRange(parameters);
			}

			return command;
		}

		public IDbCommand GenerateDelete(string table, object criteria)
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

			IDbDataParameter[] parameters = null;

			if (criteria != null)
			{
				var whereConditions = this.GetWhereConditions(criteria, out parameters);

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

			if (parameters != null)
			{
				command.Parameters.AddRange(parameters);
			}

			return command;
		}

		public IDbCommand GenerateDelete(string table, params object[] records)
		{
			ExceptionHelper.ThrowIf(() => table == null, () => new ArgumentNullException(nameof(table)));
			ExceptionHelper.ThrowIf(() => records == null, () => new ArgumentNullException(nameof(records)));
			ExceptionHelper.ThrowIf(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));
			ExceptionHelper.ThrowIf(() => records.Length == 0, () => new ArgumentException("There were no records provided.", nameof(records)));

			this.GenerateOutputTempTableArtifacts(
				table,
				true,
				out var declareTempTable,
				out var outputIntoTempTable,
				out var selectFromTempTable,
				out var joinTableToTempTable);

			var whereConditions = this.GetWhereConditions(table, records);

			var commandTextStringBuilder = new StringBuilder()
				.AppendLine(declareTempTable)
				.AppendLine()
				.AppendLine($"DELETE {table}")
				.AppendLine(outputIntoTempTable)
				.AppendLine($"WHERE {whereConditions}")
				.AppendLine()
				.AppendLine(selectFromTempTable);

			if (joinTableToTempTable != null)
			{
				commandTextStringBuilder.AppendLine(joinTableToTempTable);
			}

			return new SqlCommand(commandTextStringBuilder.ToString());
		}

		public IDbCommand GenerateUpdate(string table, object values, object criteria)
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

			var setValues = this.GetSetValues(values, out var setParameters);

			var commandTextStringBuilder = new StringBuilder()
				.AppendLine(declareTempTable)
				.AppendLine()
				.AppendLine($"UPDATE {table}")
				.AppendLine($"SET {setValues}")
				.AppendLine(outputIntoTempTable);

			IDbDataParameter[] whereParameters = null;

			if (criteria != null)
			{
				var whereConditions = this.GetWhereConditions(criteria, out whereParameters);

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

			if (whereParameters != null)
			{
				command.Parameters.AddRange(whereParameters);
			}

			return command;
		}

		public IDbCommand Generate(string commandText, object parameters, CommandType commandType)
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
		
		protected string GetQualifiedColumnValue(PropertyInfo property, object record)
		{
			object propertyValue = null;

			try
			{
				propertyValue = property?.GetValue(record);
			}
			catch (TargetException targetException)
			{
				throw new InvalidOperationException("Error getting column value from record.  Make sure all records have identical columns.", targetException);
			}

			if (propertyValue != null)
			{
				var propertyType = propertyValue.GetType();
				var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

				if (underlyingType == typeof(string) || underlyingType == typeof(DateTime))
				{
					if (underlyingType == typeof(string))
					{
						propertyValue = ((string)propertyValue).Replace("'", "''");
					}

					return $"'{propertyValue}'";
				}
				else if (underlyingType == typeof(bool))
				{
					return (bool)propertyValue ? "1" : "0";
				}
				else if (underlyingType.IsEnum)
				{
					return ((int)propertyValue).ToString();
				}
				else
				{
					return propertyValue.ToString();
				}
			}
			else
			{
				return "NULL";
			}
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

		protected string GenerateDelimitedInsertValues(object[] records, PropertyInfo[] properties)
		{
			var insertValuesStringBuilder = new StringBuilder();

			foreach (var record in records)
			{
				if (insertValuesStringBuilder.Length > 0)
				{
					insertValuesStringBuilder.Append(", ");
				}

				var delimitedValues = this.GetDelimitedRecordInsertValues(record, properties);

				insertValuesStringBuilder.Append($"({delimitedValues})");
			}

			return insertValuesStringBuilder.ToString();
		}

		protected string GetDelimitedRecordInsertValues(object record, PropertyInfo[] properties)
		{
			var recordInsertValuesStringBuilder = new StringBuilder();

			foreach (var property in properties)
			{
				if (recordInsertValuesStringBuilder.Length > 0)
				{
					recordInsertValuesStringBuilder.Append(", ");
				}

				var qualifiedValue = this.GetQualifiedColumnValue(property, record);

				recordInsertValuesStringBuilder.Append(qualifiedValue);
			}

			return recordInsertValuesStringBuilder.ToString();
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

		protected string GetWhereConditions(object criteria, out IDbDataParameter[] parameters)
		{
			var parameterList = new List<IDbDataParameter>();

			var conditionsStringBuilder = new StringBuilder();
			var properties = criteria.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

			foreach (var property in properties)
			{
				if (conditionsStringBuilder.Length > 0)
				{
					conditionsStringBuilder.Append(" AND ");
				}

				var propertyValue = property.GetValue(criteria);
				var parameterName = property.Name;

				if (propertyValue != null)
				{
					conditionsStringBuilder.Append($"{property.Name} = @{parameterName}");

					parameterList.Add(new SqlParameter(parameterName, propertyValue));
				}
				else
				{
					conditionsStringBuilder.Append($"{property.Name} IS NULL");
				}
			}

			parameters = parameterList.ToArray();

			return conditionsStringBuilder.ToString();
		}

		protected string GetWhereConditions(string table, object[] records)
		{
			var primaryKeyColumns = this.GetPrimaryKeyColumns(table);

			if (primaryKeyColumns.Count() == 1)
			{
				return this.GetWhereConditionsForSingleColumn(table, records);
			}
			else
			{
				return this.GetWhereConditionsForMultipleColumns(table, records);
			}
		}

		protected string GetWhereConditionsForSingleColumn(string table, object[] records)
		{
			var whereConditionsStringBuilder = new StringBuilder();
			var primaryKeyColumns = this.GetPrimaryKeyColumns(table);
			var primaryKeyProperty = records.First().GetType().GetProperty(primaryKeyColumns.Single());

			whereConditionsStringBuilder.Append($"{primaryKeyProperty.Name} IN (");

			foreach (var record in records)
			{
				var primaryKeyValue = this.GetQualifiedColumnValue(primaryKeyProperty, record);

				whereConditionsStringBuilder.Append(primaryKeyValue).Append(", ");
			}

			whereConditionsStringBuilder.Length -= 2;
			whereConditionsStringBuilder.Append(")");

			return whereConditionsStringBuilder.ToString();
		}

		protected string GetWhereConditionsForMultipleColumns(string table, object[] records)
		{
			var whereConditionsStringBuilder = new StringBuilder();
			var primaryKeyColumns = this.GetPrimaryKeyColumns(table);
			var properties = records.First().GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance).AsEnumerable();

			if (primaryKeyColumns.Count() > 0)
			{
				properties = properties.Where(p => primaryKeyColumns.Contains(p.Name));
			}

			foreach (var record in records)
			{
				whereConditionsStringBuilder.Append("(");

				foreach (var property in properties)
				{
					var columnValue = this.GetQualifiedColumnValue(property, record);

					if (columnValue != "NULL")
					{
						whereConditionsStringBuilder.Append($"{property.Name} = {columnValue} AND ");
					}
					else
					{
						whereConditionsStringBuilder.Append($"{property.Name} IS NULL AND ");
					}
				}

				whereConditionsStringBuilder.Length -= 5;
				whereConditionsStringBuilder.Append(") OR ");
			}

			whereConditionsStringBuilder.Length -= 4;

			return whereConditionsStringBuilder.ToString();
		}

		protected string GetSetValues(object values, out IDbDataParameter[] parameters)
		{
			var parameterList = new List<IDbDataParameter>();
			var setValuesStringBuilder = new StringBuilder();
			var properties = values.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

			foreach (var property in properties)
			{
				if (setValuesStringBuilder.Length > 0)
				{
					setValuesStringBuilder.Append(", ");
				}

				var propertyValue = property.GetValue(values);
				var parameterName = $"set_{property.Name}";

				setValuesStringBuilder.Append($"{property.Name} = @{parameterName}");

				parameterList.Add(new SqlParameter(parameterName, propertyValue ?? DBNull.Value));
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
					command.Parameters.AddWithValue("Schema", schema != null ? schema : (object)DBNull.Value);
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

		protected IEnumerable<string> GetPrimaryKeyColumns(string table)
		{
			var columnDefinitions = this.GetColumnDefinitions(table);

			return columnDefinitions.Where(x => x.IsPrimaryKey).Select(x => x.Name);
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
