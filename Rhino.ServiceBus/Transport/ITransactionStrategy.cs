using System;

namespace Rhino.ServiceBus.Transport
{
    public interface ITransactionStrategy
    {        
        IRsbTransaction Begin();
    }

    public interface IRsbTransaction : IDisposable
    {
        void Complete();
    }
}