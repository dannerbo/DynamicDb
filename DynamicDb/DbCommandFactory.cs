using System.Data;
using System.Data.SqlClient;

namespace DynamicDb
{
	public class DbCommandFactory
	{
		public DbCommandFactory(SqlTransaction transaction)
		{
			this.Transaction = transaction;
		}

		public DbCommandFactory()
		{
		}

		public SqlTransaction Transaction { get; set; }

		public virtual SqlCommand Create(string commandText, CommandType commandType, SqlParameter[] parameters)
		{
			var command = new SqlCommand(commandText);

			command.CommandType = commandType;

			if (parameters?.Length > 0)
			{
				command.Parameters.AddRange(parameters);
			}

			if (this.Transaction != null)
			{
				command.Transaction = this.Transaction;
			}

			return command;
		}
	}
}
