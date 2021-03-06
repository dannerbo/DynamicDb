﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Reflection.Emit;

namespace DynamicDb
{
	public sealed class RecordFactory
	{
		private static Dictionary<SqlConnectionStringBuilder, Cache> Caches = new Dictionary<SqlConnectionStringBuilder, Cache>();
		private SqlConnectionStringBuilder connectionStringBuilder;
		private Cache cache;

		private RecordFactory(SqlConnectionStringBuilder connectionStringBuilder, Cache cache)
		{
			this.connectionStringBuilder = connectionStringBuilder;
			this.cache = cache;
		}

		public static RecordFactory Create(string connectionString)
		{
			lock (RecordFactory.Caches)
			{
				var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);

				if (!RecordFactory.Caches.TryGetValue(connectionStringBuilder, out var cache))
				{
					cache = new Cache();

					RecordFactory.Caches.Add(connectionStringBuilder, cache);
				}

				return new RecordFactory(connectionStringBuilder, cache);
			}
		}

		public object Create(IDataReader dataReader, string table)
		{
			lock (this.cache)
			{
				var type = this.GetOrCreateType(dataReader, table);

				return this.CreateInstance(type, dataReader);
			}
		}

		public object Create(IDataReader dataReader, IDbCommand command)
		{
			lock (this.cache)
			{
				var type = this.GetOrCreateType(dataReader, command);

				return this.CreateInstance(type, dataReader);
			}
		}

		private object CreateInstance(Type type, IDataReader dataReader)
		{
			var record = Activator.CreateInstance(type);

			this.SetProperties(record, dataReader);

			return record;
		}

		private Type GetOrCreateType(IDataReader dataReader, string table)
		{
			if (!this.cache.TableTypes.TryGetValue(table, out var type))
			{
				var typeName = this.GenerateTypeName(table);

				type = this.CreateType(typeName, dataReader);

				this.cache.TableTypes.Add(table, type);

				this.AddToPropertiesCache(type);
			}

			return type;
		}

		private Type GetOrCreateType(IDataReader dataReader, IDbCommand command)
		{
			if (!this.cache.CommandTypes.TryGetValue(command.CommandText, out var type))
			{
				var typeName = this.GenerateTypeName(command);

				type = this.CreateType(typeName, dataReader);

				this.cache.CommandTypes.Add(command.CommandText, type);

				this.AddToPropertiesCache(type);
			}

			return type;
		}

		private Type CreateType(string typeName, IDataReader dataReader)
		{
			if (this.cache.AssemblyName == null)
			{
				this.cache.AssemblyName = new AssemblyName(nameof(DynamicDb));
				this.cache.AssemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(this.cache.AssemblyName, AssemblyBuilderAccess.Run);
				this.cache.ModuleBuilder = this.cache.AssemblyBuilder.DefineDynamicModule(this.cache.AssemblyName.Name);
			}

			var typeBuilder = this.cache.ModuleBuilder.DefineType(typeName, TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.AutoClass | TypeAttributes.AnsiClass | TypeAttributes.BeforeFieldInit | TypeAttributes.AutoLayout);

			for (int i = 0; i < dataReader.FieldCount; i++)
			{
				var fieldName = dataReader.GetName(i);
				var fieldType = dataReader.GetFieldType(i);

				if (fieldType.IsValueType)
				{
					var schemaTable = dataReader.GetSchemaTable();
					var isNullable = (bool)schemaTable.Rows[i]["AllowDBNUll"];

					if (isNullable)
					{
						fieldType = typeof(Nullable<>).MakeGenericType(fieldType);
					}
				}

				var fieldBuilder = typeBuilder.DefineField("_" + fieldName, fieldType, FieldAttributes.Private);

				var getPropertyMethodBuilder = typeBuilder.DefineMethod("get_" + fieldName, MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, fieldType, Type.EmptyTypes);
				var propertyGetIL = getPropertyMethodBuilder.GetILGenerator();

				propertyGetIL.Emit(OpCodes.Ldarg_0);
				propertyGetIL.Emit(OpCodes.Ldfld, fieldBuilder);
				propertyGetIL.Emit(OpCodes.Ret);

				var setPropertyMethodBuilder = typeBuilder.DefineMethod("set_" + fieldName, MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, null, new Type[] { fieldType });
				var propertySetIL = setPropertyMethodBuilder.GetILGenerator();

				propertySetIL.Emit(OpCodes.Ldarg_0);
				propertySetIL.Emit(OpCodes.Ldarg_1);
				propertySetIL.Emit(OpCodes.Stfld, fieldBuilder);
				propertySetIL.Emit(OpCodes.Ret);

				var propertyBuilder = typeBuilder.DefineProperty(fieldName, System.Reflection.PropertyAttributes.None, fieldType, null);

				propertyBuilder.SetGetMethod(getPropertyMethodBuilder);
				propertyBuilder.SetSetMethod(setPropertyMethodBuilder);
			}

			return typeBuilder.CreateType();
		}

		private string GenerateTypeName(string databaseObject)
		{
			return $"{this.connectionStringBuilder.InitialCatalog}_{databaseObject.Replace('.', '_')}";
		}

		private string GenerateTypeName(IDbCommand command)
		{
			return command.CommandType == CommandType.Text
				? $"QueryResult_{Guid.NewGuid()}"
				: this.GenerateTypeName(command.CommandText);
		}

		private void AddToPropertiesCache(Type type)
		{
			var propertiesDictionary = new Dictionary<string, PropertyInfo>();
			var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

			foreach (var property in properties)
			{
				propertiesDictionary.Add(property.Name, property);
			}

			this.cache.TypeProperties.Add(type, propertiesDictionary);
		}

		private void SetProperties(object record, IDataReader dataReader)
		{
			for (int i = 0; i < dataReader.FieldCount; i++)
			{
				var fieldType = dataReader.GetFieldType(i);
				var fieldName = dataReader.GetName(i);
				var property = this.cache.TypeProperties[record.GetType()][fieldName];

				property.SetValue(record, !dataReader.IsDBNull(i) ? dataReader.GetValue(i) : null);
			}
		}

		private class Cache
		{
			public Dictionary<string, Type> TableTypes { get; set; } = new Dictionary<string, Type>();
			public Dictionary<string, Type> CommandTypes { get; set; } = new Dictionary<string, Type>();
			public Dictionary<Type, Dictionary<string, PropertyInfo>> TypeProperties { get; set; } = new Dictionary<Type, Dictionary<string, PropertyInfo>>();
			public AssemblyName AssemblyName { get; set; }
			public AssemblyBuilder AssemblyBuilder { get; set; }
			public ModuleBuilder ModuleBuilder { get; set; }
		}
	}
}
