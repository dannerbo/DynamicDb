using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DynamicDb
{
	public class DbCommandGenerator
	{
		public DbCommandGenerator(TableMetadataProvider tableMetadataProvider, DbCommandFactory commandFactory)
		{
			this.TableMetadataProvider = tableMetadataProvider;
			this.CommandFactory = commandFactory;
		}

		public DbCommandGenerator(TableMetadataProvider tableMetadataProvider)
			: this(tableMetadataProvider, new DbCommandFactory())
		{
		}

		protected TableMetadataProvider TableMetadataProvider { get; private set; }
		protected DbCommandFactory CommandFactory { get; private set; }

		public SqlCommand GenerateInsert(string table, params object[] records)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => records == null, () => new ArgumentNullException(nameof(records)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));
			Throw.If(() => records.Length == 0, () => new ArgumentException("There were no records provided.", nameof(records)));

			this.GenerateOutputTempTableArtifacts(
				table,
				false,
				out var declareTempTable,
				out var outputIntoTempTable,
				out var selectFromTempTable,
				out var joinTableToTempTable);

			var properties = records.First().GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
			var columnsDelimited = String.Join(", ", properties.Select(p => $"[{p.Name}]"));
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

			return this.CommandFactory.Create(commandTextStringBuilder.ToString(), CommandType.Text, parameters);
		}

		public SqlCommand GenerateSelect(string table, params object[] criteria)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

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

			return this.CommandFactory.Create(commandTextStringBuilder.ToString(), CommandType.Text, parameters);
		}

		public SqlCommand GenerateDelete(string table, params object[] criteria)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));
			
			SqlParameter[] parameters = null;
			var whereConditions = criteria?.Length > 0 ? this.GenerateWhereConditions(criteria, out parameters) : null;
			var commandText = this.GenerateDeleteCommandText(table, whereConditions);

			return this.CommandFactory.Create(commandText, CommandType.Text, parameters);
		}

		public SqlCommand GenerateDeleteByPrimaryKeys(string table, object[] records)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));
			Throw.If(() => records == null, () => new ArgumentNullException(nameof(records)));
			Throw.If(() => records.Length == 0, () => new ArgumentException("Records were not provided.", nameof(records)));
			
			var primaryKeyColumns = this.TableMetadataProvider.GetColumnDefinitions(table).Where(x => x.IsPrimaryKey).Select(x => x.Name);

			SqlParameter[] parameters;

			var whereConditions = primaryKeyColumns.Count() > 0
				? this.GenerateWhereConditions(records, field => primaryKeyColumns.Contains(field), out parameters)
				: this.GenerateWhereConditions(records, out parameters);

			var commandText = this.GenerateDeleteCommandText(table, whereConditions);

			return this.CommandFactory.Create(commandText, CommandType.Text, parameters);
		}
		
		public SqlCommand GenerateUpdate(string table, object values, params object[] criteria)
		{
			Throw.If(() => table == null, () => new ArgumentNullException(nameof(table)));
			Throw.If(() => values == null, () => new ArgumentNullException(nameof(values)));
			Throw.If(() => table.Length == 0, () => new ArgumentException("Table name was not provided.", nameof(table)));

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

			var parameters = whereParameters?.Length > 0
				? setParameters.Union(whereParameters)
				: setParameters;

			return this.CommandFactory.Create(commandTextStringBuilder.ToString(), CommandType.Text, parameters.ToArray());
		}

		public SqlCommand Generate(string commandText, object parameters, CommandType commandType)
		{
			Throw.If(() => commandText == null, () => new ArgumentNullException(nameof(commandText)));
			Throw.If(() => commandText.Length == 0, () => new ArgumentException("Command text was not provided.", nameof(commandText)));

			List<SqlParameter> parameterList = null;

			if (parameters != null)
			{
				parameterList = new List<SqlParameter>();

				var properties = parameters.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

				foreach (var property in properties)
				{
					var propertyValue = property.GetValue(parameters);

					parameterList.Add(new SqlParameter(property.Name, propertyValue ?? DBNull.Value));
				}
			}

			return this.CommandFactory.Create(commandText, commandType, parameterList?.ToArray());
		}

		protected string GenerateDeleteCommandText(string table, string whereConditions)
		{
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

			if (!String.IsNullOrEmpty(whereConditions))
			{
				commandTextStringBuilder.AppendLine($"WHERE {whereConditions}");
			}

			commandTextStringBuilder
				.AppendLine()
				.AppendLine(selectFromTempTable);

			return commandTextStringBuilder.ToString();
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
						delimitedColumnDefinitions.Append($"[{column.Name}] {dataType}");

						break;
					}

					case "DECIMAL":
					case "NUMERIC":
					{
						delimitedColumnDefinitions.Append($"[{column.Name}] {dataType}({column.Precision}, {column.Scale})");

						break;
					}

					case "DATETIME2":
					case "DATETIMEOFFSET":
					case "FLOAT":
					case "REAL":
					{
						delimitedColumnDefinitions.Append($"[{column.Name}] {dataType}({column.Precision})");

						break;
					}

					case "TIME":
					{
						delimitedColumnDefinitions.Append($"[{column.Name}] {dataType}({column.Scale})");

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

						delimitedColumnDefinitions.Append($"[{column.Name}] {dataType}({columnMaximumLength})");

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

			var columnDefinitions = this.TableMetadataProvider.GetColumnDefinitions(table);
			var primaryKeyColumnDefinitions = columnDefinitions.Where(x => x.IsPrimaryKey);
			var idColumnDefinitions = !isForDelete && primaryKeyColumnDefinitions.Count() > 0 ? primaryKeyColumnDefinitions : columnDefinitions;
			var columnDefinitionsDelimited = this.GenerateDelimitedColumnDefinitions(idColumnDefinitions);

			declareTempTable = $"DECLARE @Record TABLE ({columnDefinitionsDelimited})";

			var outputValues = isForDelete ? "DELETED" : "INSERTED";
			var outputColumnsDelimited = String.Join(", ", idColumnDefinitions.Select(c => $"{outputValues}.[{c.Name}]"));

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

				joinConditionsStringBuilder.Append($"{otherTable}.[{column.Name}] = {table}.[{column.Name}]");
			}

			return joinConditionsStringBuilder.ToString();
		}

		protected string GenerateWhereConditions(object[] criteria, Predicate<string> fieldPredicate, out SqlParameter[] parameters)
		{
			var parameterList = new List<SqlParameter>();
			var whereConditionsStringBuilder = new StringBuilder();

			for (int i = 0; i < criteria.Length; i++)
			{
				var item = criteria[i];
				var properties = item.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
				var fieldCount = 0;

				whereConditionsStringBuilder.Append(i == 0 ? "(" : " OR (");

				for (int j = 0; j < properties.Length; j++)
				{
					var property = properties[j];

					if (fieldPredicate(property.Name))
					{
						var propertyValue = property.GetValue(item);

						if (fieldCount++ > 0)
						{
							whereConditionsStringBuilder.Append(" AND ");
						}

						if (propertyValue != null)
						{
							var parameterName = $"{property.Name}_{i}";

							whereConditionsStringBuilder.Append($"[{property.Name}] = @{parameterName}");

							parameterList.Add(new SqlParameter(parameterName, propertyValue));
						}
						else
						{
							whereConditionsStringBuilder.Append($"[{property.Name}] IS NULL");
						}
					}
				}

				whereConditionsStringBuilder.Append(")");
			}

			parameters = parameterList.ToArray();

			return whereConditionsStringBuilder.ToString();
		}

		protected string GenerateWhereConditions(object[] criteria, out SqlParameter[] parameters)
		{
			return this.GenerateWhereConditions(criteria, field => true, out parameters);
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

					setValuesStringBuilder.Append($"[{property.Name}] = @{parameterName}");

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
	}
}
