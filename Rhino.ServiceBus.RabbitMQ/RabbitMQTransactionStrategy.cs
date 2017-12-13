using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class RabbitMQTransactionStrategy : ITransactionStrategy
    {
        public IRsbTransaction Begin()
        {
            return RabbitMQTransaction.Begin();
        }
   }
}