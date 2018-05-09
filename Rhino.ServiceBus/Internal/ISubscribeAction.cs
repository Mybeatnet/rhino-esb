using System;

namespace Rhino.ServiceBus.Internal
{
    public interface ISubscribeAction
    {
        void Subscribe(Type type, Endpoint endpoint);
        void Unsubscribe(Type type, Endpoint endpoint);
        void Init(IServiceBus bus);
    }
}