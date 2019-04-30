using System;
using System.Threading;

namespace Rhino.ServiceBus.RabbitMQ.Tests
{
    public class InstanceConsumer : ConsumerOf<int>
    {
        private readonly ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);

        private int _value;

        public void Consume(int message)
        {
            _value = message;
            _waitHandle.Set();
        }

        public int Wait()
        {
            if (!_waitHandle.Wait(5000))
                throw new TimeoutException("Did not receive message within 5 seconds");

            return _value;
        }
    }
}