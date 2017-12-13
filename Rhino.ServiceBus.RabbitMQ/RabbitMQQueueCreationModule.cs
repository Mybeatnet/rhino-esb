using System;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Msmq;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQQueueCreationModule : IServiceBusAware
    {
        private readonly RabbitMQQueueStrategy _queueStrategy;

        public RabbitMQQueueCreationModule(RabbitMQQueueStrategy queueStrategy)
        {
            _queueStrategy = queueStrategy;
        }

        public void BusStarting(IServiceBus bus)
        {
            try
            {
                _queueStrategy.InitializeQueue();
            }
            catch (Exception e)
            {
                throw new TransportException(
                    "Could not open queue: " + bus.Endpoint + Environment.NewLine +
                    "Queue path: " + MsmqUtil.GetQueuePath(bus.Endpoint), e);
            }
        }

        public void BusStarted(IServiceBus bus)
        {
        }

        public void BusDisposing(IServiceBus bus)
        {
        }

        public void BusDisposed(IServiceBus bus)
        {
        }
    }
}