using System.Data;
using System.Data.SqlClient;

namespace DynamicDb
{
	public interface IDbCommandFactory
	{
		SqlCommand Create(string commandText, CommandType commandType, SqlParameter[] parameters);
	}
}
