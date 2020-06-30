using System;
using System.Messaging;
using System.Threading;
using System.Web;

namespace Rhino.ServiceBus.Transport
{
    public class MsmqTransactionStrategy : ITransactionStrategy
    {
        private static readonly string Key = typeof(MsmqTransactionStrategy).FullName + ".Current";

        private static readonly AsyncLocal<RsbTransaction> _currentTx = new AsyncLocal<RsbTransaction>();

        public static MessageQueueTransaction Current => GetTx();

        public IRsbTransaction Begin()
        {
            var tx = new MessageQueueTransaction();
            tx.Begin();
            return _currentTx.Value = new RsbTransaction(tx);
        }

        public void Send(MessageQueue queue, Message msg)
        {
            var tx = GetTx();
            if (tx == null)
                queue.Send(msg, MessageQueueTransactionType.Single);
            else
                queue.Send(msg, tx);
        }

        private static MessageQueueTransaction GetTx()
        {
            if (HttpContext.Current == null)
                return _currentTx?.Value?.Transaction;

            var tx = (RsbTransaction) HttpContext.Current.Items[Key];
            return tx?.Transaction;
        }

        public Message ReceiveById(MessageQueue queue, string messageId)
        {
            var tx = GetTx();
            if (tx == null)
                return queue.ReceiveById(messageId);
            else
                return queue.ReceiveById(messageId, tx);
        }

        public Message Receive(MessageQueue queue)
        {
            var tx = GetTx();
            if (tx == null)
                return queue.Receive();
            else
                return queue.Receive(tx);
        }

        private class RsbTransaction : IRsbTransaction
        {
            public MessageQueueTransaction Transaction { get; private set; }

            public RsbTransaction(MessageQueueTransaction transaction)
            {
                Transaction = transaction;
            }

            public void Dispose()
            {
                _currentTx.Value = null;
                if (Transaction.Status == MessageQueueTransactionStatus.Pending)
                    Transaction.Abort();
                Transaction.Dispose();
            }

            public void Complete()
            {
                Transaction.Commit();
            }
        }
    }
}