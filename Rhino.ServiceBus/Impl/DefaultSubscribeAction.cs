using System;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Messages;

namespace Rhino.ServiceBus.Impl
{
    public class DefaultSubscribeAction : ISubscribeAction
    {
        private IServiceBus _bus;

        public void Init(IServiceBus bus)
        {
            _bus = bus;
        }

        public void Invoke(Type type, Endpoint endpoint)
        {
            _bus.Send(endpoint, new AddSubscription
            {
                Endpoint = _bus.Endpoint,
                Type = type.FullName
            });
        }
    }
}