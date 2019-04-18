namespace DynamicDb
{
	public class SqlColumn
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
