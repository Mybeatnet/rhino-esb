using System.Messaging;
using System.Transactions;

namespace Rhino.ServiceBus.Transport
{
    public class TransactionScopeStrategy : ITransactionStrategy
    {
        public IRsbTransaction Begin()
        {
            var transactionOptions = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.ReadCommitted,
                Timeout = TransportUtil.GetTransactionTimeout()
            };
            var tx = new TransactionScope(TransactionScopeOption.Required, transactionOptions);
            return new RsbTransaction(tx);
        }

        public void Send(MessageQueue queue, Message msg)
        {
            queue.Send(msg, GetTransactionType());
        }

        private MessageQueueTransactionType GetTransactionType()
        {
            if (Transaction.Current == null)
                return MessageQueueTransactionType.Single;
            return MessageQueueTransactionType.Automatic;
        }

        public Message ReceiveById(MessageQueue queue, string messageId)
        {
            return queue.ReceiveById(
                messageId,
                GetTransactionType());
        }

        public Message Receive(MessageQueue queue)
        {
            return queue.Receive(GetTransactionType());
        }

        private class RsbTransaction : IRsbTransaction
        {
            private readonly TransactionScope _tx;

            public RsbTransaction(TransactionScope tx)
            {
                _tx = tx;
            }

            public void Dispose()
            {
                _tx.Dispose();
            }

            public void Complete()
            {
                _tx.Complete();
            }
        }
    }
}