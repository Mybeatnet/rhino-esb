using System;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQConsumer
    {
        private static readonly ILog _log = LogManager.GetLogger<RabbitMQConsumer>();
        private readonly Action<IModel, BasicDeliverEventArgs> _callback;
        private readonly Endpoint _endpoint;
        private readonly RabbitMQConnectionProvider _rabbitMQConnectionProvider;

        private readonly int _threadCount;
        private readonly ITransactionStrategy _txStrategy;
        private long _busy;

        private IModel _model;
        private readonly ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);

        public RabbitMQConsumer(
            int threadCount,
            RabbitMQConnectionProvider rabbitMQConnectionProvider,
            ITransactionStrategy txStrategy,
            Endpoint endpoint,
            Action<IModel, BasicDeliverEventArgs> callback)
        {
            _threadCount = threadCount;
            _rabbitMQConnectionProvider = rabbitMQConnectionProvider;
            _txStrategy = txStrategy;
            _endpoint = endpoint;
            _callback = callback;
        }

        public void Start()
        {
            ReceiveMessage();
        }

        private void ReceiveMessage()
        {
            if (_threadCount == 0)
                return;

            try
            {
                var address = RabbitMQAddress.From(_endpoint.Uri);
                // don't open it transactional - we use ack or nack for "transactionality" on the receiving model
                _model = _rabbitMQConnectionProvider.Open(address, false);
                _model.BasicQos(0, 10, false);
                var consumer = new EventingBasicConsumer(_model);
                consumer.Received += (o, e) =>
                {
                    try
                    {
                        var current = Interlocked.Increment(ref _busy);
                        if (current > _threadCount)
                        {
                            _waitHandle.Wait();
                            _waitHandle.Reset();
                        }

                        _log.DebugFormat("Received message {0} ({1} in progress)", e.BasicProperties.MessageId,
                            current);

                        Task.Factory.StartNew(() =>
                        {
                            var doCommit = false;
                            try
                            {
                                var tx = (RabbitMQTransaction) _txStrategy.Begin();
                                tx.Enlist(commit => doCommit = commit);
                                _callback(_model, e);
                            }
                            catch (Exception ex)
                            {
                                _log.Error($"Error processing message {e.BasicProperties.MessageId}", ex);
                            }
                            finally
                            {
                                if (doCommit)
                                    _model.BasicAck(e.DeliveryTag, false);
                                else
                                    _model.BasicNack(e.DeliveryTag, false, true);

                                Interlocked.Decrement(ref _busy);
                                _waitHandle.Set();
                            }
                        });
                    }
                    catch (Exception ex)
                    {
                        _log.Fatal("Unhandled exception in consumer", ex);
                    }
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