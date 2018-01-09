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

        private readonly int _index;
        private readonly RabbitMQConnectionProvider _rabbitMQConnectionProvider;
        private readonly Endpoint _endpoint;
        private Thread _thread;
        private readonly Action<IModel, BasicDeliverEventArgs> _callback;
        private IModel _model;
        private long _busy = 0;

        public RabbitMQConsumer(int index, RabbitMQConnectionProvider rabbitMQConnectionProvider, Endpoint endpoint, Action<IModel, BasicDeliverEventArgs> callback)
        {
            _index = index;
            _rabbitMQConnectionProvider = rabbitMQConnectionProvider;
            _endpoint = endpoint;
            _callback = callback;
        }

        public void Start()
        {
            _thread = new Thread(ReceiveMessage)
            {
                Name = "Rhino Service Bus Worker Thread #" + _index,
                IsBackground = true
            };
            _thread.Start();
        }

        private void ReceiveMessage()
        {
            try
            {
                var address = RabbitMQAddress.From(_endpoint.Uri);
                _model = _rabbitMQConnectionProvider.Open(address, _endpoint.Transactional ?? true);
                _model.BasicQos(0, 100, false);
                var consumer = new EventingBasicConsumer(_model);
                consumer.Received += (o, e) =>
                {
                    Interlocked.Increment(ref _busy);

                    _callback(_model, e);

                    Interlocked.Decrement(ref _busy);
                };
                _model.BasicConsume(address.QueueName, false, consumer);
            }
            catch (Exception ex)
            {
                _log.Error("Error receiving messages", ex);
            }
        }

        public void Stop()
        {
            _model?.Dispose();
            _model = null;
            while (Interlocked.Read(ref _busy) > 0)
                Thread.Sleep(200);
        }
    }
}