using System;
using System.Threading;
namespace Tlfs
{
    enum LogLevel
    {
        DEBUG,
        INFO,
        ERROR
    }
    static class Log
    {

        public static LogLevel CurrentLevel = LogLevel.DEBUG;

        public static void Add(LogLevel level, string message)
        {
            if (level >= CurrentLevel)
            {
                Console.WriteLine($"{level.ToString()}: {Thread.CurrentThread.ManagedThreadId} {message}");
            }
        }
    }
}