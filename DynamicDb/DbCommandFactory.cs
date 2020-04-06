using System.Data;
using System.Data.SqlClient;

namespace DynamicDb
{
	public class DbCommandFactory : IDbCommandFactory
	{
		public SqlTransaction Transaction { get; set; }

		public virtual SqlCommand Create(string commandText, CommandType commandType, SqlParameter[] parameters)
		{
			var command = new SqlCommand(commandText);

			command.CommandType = commandType;

			if (parameters?.Length > 0)
			{
				command.Parameters.AddRange(parameters);
			}

			return command;
		}
	}
}
