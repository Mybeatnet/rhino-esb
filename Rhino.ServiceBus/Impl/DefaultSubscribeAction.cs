using System;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Messages;

namespace Rhino.ServiceBus.Impl
{
    public class DefaultSubscribeAction : ISubscribeAction
    {
        private IServiceBus _bus;

        public void Unsubscribe(Type type, Endpoint endpoint)
        {
            _bus.Send(endpoint, new RemoveSubscription
            {
                Endpoint = _bus.Endpoint,
                Type = type.FullName
            });
        }

        public void Init(IServiceBus bus)
        {
            _bus = bus;
        }

        public void Subscribe(Type type, Endpoint endpoint)
        {
            _bus.Send(endpoint, new AddSubscription
            {
                Endpoint = _bus.Endpoint,
                Type = type.FullName
            });
        }
    }
}