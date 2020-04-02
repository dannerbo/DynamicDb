using System;

namespace DynamicDb
{
	public class Throw
	{
		public static void If(Func<bool> condition, Func<Exception> exceptionToThrow)
		{
			if (condition())
			{
				throw exceptionToThrow();
			}
		}
	}
}
