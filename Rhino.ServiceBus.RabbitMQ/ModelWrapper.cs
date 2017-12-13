using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Rhino.ServiceBus.RabbitMQ
{
    public class ModelWrapper : IModel
    {
        private readonly IModel _impl;

        public ModelWrapper(IModel model)
        {
            _impl = model;
        }

        public void Dispose()
        {
            // dispose handled by outer transaction
        }

        public void Abort()
        {
            _impl.Abort();
        }

        public void Abort(ushort replyCode, string replyText)
        {
            _impl.Abort(replyCode, replyText);
        }

        public void BasicAck(ulong deliveryTag, bool multiple)
        {
            _impl.BasicAck(deliveryTag, multiple);
        }

        public void BasicCancel(string consumerTag)
        {
            _impl.BasicCancel(consumerTag);
        }

        public string BasicConsume(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            return _impl.BasicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, consumer);
        }

        public BasicGetResult BasicGet(string queue, bool autoAck)
        {
            return _impl.BasicGet(queue, autoAck);
        }

        public void BasicNack(ulong deliveryTag, bool multiple, bool requeue)
        {
            _impl.BasicNack(deliveryTag, multiple, requeue);
        }

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties, byte[] body)
        {
            _impl.BasicPublish(exchange, routingKey, mandatory, basicProperties, body);
        }

        public void BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            _impl.BasicQos(prefetchSize, prefetchCount, global);
        }

        public void BasicRecover(bool requeue)
        {
            _impl.BasicRecover(requeue);
        }

        public void BasicRecoverAsync(bool requeue)
        {
            _impl.BasicRecoverAsync(requeue);
        }

        public void BasicReject(ulong deliveryTag, bool requeue)
        {
            _impl.BasicReject(deliveryTag, requeue);
        }

        public void Close()
        {
            _impl.Close();
        }

        public void Close(ushort replyCode, string replyText)
        {
            _impl.Close(replyCode, replyText);
        }

        public void ConfirmSelect()
        {
            _impl.ConfirmSelect();
        }

        public IBasicProperties CreateBasicProperties()
        {
            return _impl.CreateBasicProperties();
        }

        public void ExchangeBind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _impl.ExchangeBind(destination, source, routingKey, arguments);
        }

        public void ExchangeBindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _impl.ExchangeBindNoWait(destination, source, routingKey, arguments);
        }

        public void ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments)
        {
            _impl.ExchangeDeclare(exchange, type, durable, autoDelete, arguments);
        }

        public void ExchangeDeclareNoWait(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments)
        {
            _impl.ExchangeDeclareNoWait(exchange, type, durable, autoDelete, arguments);
        }

        public void ExchangeDeclarePassive(string exchange)
        {
            _impl.ExchangeDeclarePassive(exchange);
        }

        public void ExchangeDelete(string exchange, bool ifUnused)
        {
            _impl.ExchangeDelete(exchange, ifUnused);
        }

        public void ExchangeDeleteNoWait(string exchange, bool ifUnused)
        {
            _impl.ExchangeDeleteNoWait(exchange, ifUnused);
        }

        public void ExchangeUnbind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _impl.ExchangeUnbind(destination, source, routingKey, arguments);
        }

        public void ExchangeUnbindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _impl.ExchangeUnbindNoWait(destination, source, routingKey, arguments);
        }

        public void QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            _impl.QueueBind(queue, exchange, routingKey, arguments);
        }

        public void QueueBindNoWait(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            _impl.QueueBindNoWait(queue, exchange, routingKey, arguments);
        }

        public QueueDeclareOk QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments)
        {
            return _impl.QueueDeclare(queue, durable, exclusive, autoDelete, arguments);
        }

        public void QueueDeclareNoWait(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments)
        {
            _impl.QueueDeclareNoWait(queue, durable, exclusive, autoDelete, arguments);
        }

        public QueueDeclareOk QueueDeclarePassive(string queue)
        {
            return _impl.QueueDeclarePassive(queue);
        }

        public uint MessageCount(string queue)
        {
            return _impl.MessageCount(queue);
        }

        public uint ConsumerCount(string queue)
        {
            return _impl.ConsumerCount(queue);
        }

        public uint QueueDelete(string queue, bool ifUnused, bool ifEmpty)
        {
            return _impl.QueueDelete(queue, ifUnused, ifEmpty);
        }

        public void QueueDeleteNoWait(string queue, bool ifUnused, bool ifEmpty)
        {
            _impl.QueueDeleteNoWait(queue, ifUnused, ifEmpty);
        }

        public uint QueuePurge(string queue)
        {
            return _impl.QueuePurge(queue);
        }

        public void QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            _impl.QueueUnbind(queue, exchange, routingKey, arguments);
        }

        public void TxCommit()
        {
            _impl.TxCommit();
        }

        public void TxRollback()
        {
            _impl.TxRollback();
        }

        public void TxSelect()
        {
            _impl.TxSelect();
        }

        public bool WaitForConfirms()
        {
            return _impl.WaitForConfirms();
        }

        public bool WaitForConfirms(TimeSpan timeout)
        {
            return _impl.WaitForConfirms(timeout);
        }

        public bool WaitForConfirms(TimeSpan timeout, out bool timedOut)
        {
            return _impl.WaitForConfirms(timeout, out timedOut);
        }

        public void WaitForConfirmsOrDie()
        {
            _impl.WaitForConfirmsOrDie();
        }

        public void WaitForConfirmsOrDie(TimeSpan timeout)
        {
            _impl.WaitForConfirmsOrDie(timeout);
        }

        public int ChannelNumber
        {
            get { return _impl.ChannelNumber; }
        }

        public ShutdownEventArgs CloseReason
        {
            get { return _impl.CloseReason; }
        }

        public IBasicConsumer DefaultConsumer
        {
            get { return _impl.DefaultConsumer; }
            set { _impl.DefaultConsumer = value; }
        }

        public bool IsClosed
        {
            get { return _impl.IsClosed; }
        }

        public bool IsOpen
        {
            get { return _impl.IsOpen; }
        }

        public ulong NextPublishSeqNo
        {
            get { return _impl.NextPublishSeqNo; }
        }

        public TimeSpan ContinuationTimeout
        {
            get { return _impl.ContinuationTimeout; }
            set { _impl.ContinuationTimeout = value; }
        }

        public event EventHandler<BasicAckEventArgs> BasicAcks
        {
            add { _impl.BasicAcks += value; }
            remove { _impl.BasicAcks -= value; }
        }

        public event EventHandler<BasicNackEventArgs> BasicNacks
        {
            add { _impl.BasicNacks += value; }
            remove { _impl.BasicNacks -= value; }
        }

        public event EventHandler<EventArgs> BasicRecoverOk
        {
            add { _impl.BasicRecoverOk += value; }
            remove { _impl.BasicRecoverOk -= value; }
        }

        public event EventHandler<BasicReturnEventArgs> BasicReturn
        {
            add { _impl.BasicReturn += value; }
            remove { _impl.BasicReturn -= value; }
        }

        public event EventHandler<CallbackExceptionEventArgs> CallbackException
        {
            add { _impl.CallbackException += value; }
            remove { _impl.CallbackException -= value; }
        }

        public event EventHandler<FlowControlEventArgs> FlowControl
        {
            add { _impl.FlowControl += value; }
            remove { _impl.FlowControl -= value; }
        }

        public event EventHandler<ShutdownEventArgs> ModelShutdown
        {
            add { _impl.ModelShutdown += value; }
            remove { _impl.ModelShutdown -= value; }
        }
    }
}