using System.Data;
using System.Data.SqlClient;

namespace DynamicDb
{
	public interface IDbCommandGenerator
	{
		SqlCommand Generate(string commandText, object parameters, CommandType commandType);
		SqlCommand GenerateDelete(string table, params object[] criteria);
		SqlCommand GenerateDeleteByPrimaryKeys(string table, object[] records);
		SqlCommand GenerateInsert(string table, params object[] records);
		SqlCommand GenerateSelect(string table, params object[] criteria);
		SqlCommand GenerateUpdate(string table, object values, params object[] criteria);
	}
}
