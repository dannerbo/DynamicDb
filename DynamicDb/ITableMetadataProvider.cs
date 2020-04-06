using System.Collections.Generic;

namespace DynamicDb
{
	public interface ITableMetadataProvider
	{
		IEnumerable<SqlColumn> GetColumnDefinitions(string table);
	}
}
