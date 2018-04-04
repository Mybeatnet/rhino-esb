using System;

namespace Rhino.ServiceBus.Internal
{
    public interface ISubscribeAction
    {
        void Invoke(Type type, Endpoint endpoint);
        void Init(IServiceBus bus);
    }
}