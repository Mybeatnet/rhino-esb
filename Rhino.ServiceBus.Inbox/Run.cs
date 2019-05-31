using System;
using System.Collections.Generic;

namespace Rhino.ServiceBus.Inbox
{
    internal static class Run
    {
        public static void All(params Action[] actions)
        {
            var exceptions = new List<Exception>();
            foreach (var act in actions)
            {
                try
                {
                    act();
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }
            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        public static IDisposable OnDispose(Action callback)
        {
            return new RunOnDispose(callback);
        }

        private class RunOnDispose : IDisposable
        {
            private readonly Action _callback;

            public RunOnDispose(Action callback)
            {
                _callback = callback;
            }

            public void Dispose()
            {
                _callback();
            }
        }
    }
}