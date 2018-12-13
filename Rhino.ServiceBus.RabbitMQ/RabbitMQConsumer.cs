using System;
using System.Threading;
using Common.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQConsumer
    {
        private static readonly ILog _log = LogManager.GetLogger<RabbitMQConsumer>();

        private readonly int _threadCount;
        private readonly RabbitMQConnectionProvider _rabbitMQConnectionProvider;
        private readonly Endpoint _endpoint;        
        private readonly Action<IModel, BasicDeliverEventArgs> _callback;

        private IModel _model;
        private long _busy = 0;

        public RabbitMQConsumer(int threadCount,
            RabbitMQConnectionProvider rabbitMQConnectionProvider,
            Endpoint endpoint, 
            Action<IModel, BasicDeliverEventArgs> callback)
        {
            _threadCount = threadCount;
            _rabbitMQConnectionProvider = rabbitMQConnectionProvider;
            _endpoint = endpoint;
            _callback = callback;
        }

        public void Start()
        {
            ReceiveMessage();
        }

        private void ReceiveMessage()
        {
            try
            {
                var address = RabbitMQAddress.From(_endpoint.Uri);
                // don't open it transactional - we use ack or nack for "transactionality" on the receiving model
                _model = _rabbitMQConnectionProvider.Open(address, false);
                _model.BasicQos(0, (ushort) _threadCount, false);
                var consumer = new EventingBasicConsumer(_model);
                consumer.Received += (o, e) =>
                {
                    try
                    {
                        var current = Interlocked.Increment(ref _busy);
                        _log.DebugFormat("Received message {0} ({1} in progress)", e.BasicProperties.MessageId,
                            current);

                        try
                        {
                            _callback(_model, e);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _busy);
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Fatal("Unhandled exception in consumer", ex);
                        Restart();
                    }
                };
                _model.BasicConsume(address.QueueName, false, consumer);
            }
            catch (Exception ex)
            {
                _log.Error("Error receiving messages", ex);
            }
        }

        private void Restart()
        {
            try
            {
                using (_model)
                    _model.Close();
            }
            catch
            {

            }
        }
        public void Stop()
        {
            _model?.Close();
            _model?.Dispose();
            for (var i = 0; i < 300; i++)
            {
                if (Interlocked.Read(ref _busy) == 0)
                    break;

                Thread.Sleep(100);
            }

            _model = null;
        }
    }
}