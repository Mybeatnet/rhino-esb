using System;
using System.Collections.Generic;

namespace Rhino.ServiceBus.Impl
{
    public interface IMessageOwnersSelector
    {
        Endpoint GetEndpointForMessageBatch(object[] messages);
        IEnumerable<MessageOwner> Of(Type type);
        IEnumerable<MessageOwner> NotOf(Type type);
    }
}