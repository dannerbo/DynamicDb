using System;

namespace DynamicDb
{
	public class ExceptionHelper
	{
		public static void ThrowIf(Func<bool> condition, Func<Exception> exceptionToThrow)
		{
			if (condition())
			{
				throw exceptionToThrow();
			}
		}
	}
}
