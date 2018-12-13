using System;
using System.Collections.Generic;
using Common.Logging;
using RabbitMQ.Client;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQTransaction : IRsbTransaction
    {
        private static readonly ILog _log = LogManager.GetLogger<RabbitMQTransaction>();

        [ThreadStatic] private static RabbitMQTransaction _current;
        private bool _commit;
        private Action<bool> _completions;
        private bool _disposed;

        private readonly ISet<IModel> _models = new HashSet<IModel>();

        public static RabbitMQTransaction Current
        {
            get { return _current; }
        }

        public void Dispose()
        {
            if (_disposed) throw new ObjectDisposedException("Transaction is already disposed, cannot call Dispose twice");
            _disposed = true;

            try
            {
                if (_completions != null)
                {
                    foreach (Action<bool> action in _completions.GetInvocationList())
                    {
                        try
                        {
                            action(_commit);
                        }
                        catch (Exception ex)
                        {
                            _log.Fatal("Error performing completions: commit=" + _commit, ex);
                        }
                    }

                    _completions = null;
                }

                _current = null;
                foreach (var model in _models)
                {
                    if (_commit)
                        model.TxCommit();
                    //else
                    //    model.TxRollback();

                    model.Dispose();
                }
            }
            catch (Exception ex)
            {
                _log.Fatal("Error disposing transaction: commit=" + _commit, ex);
            }
        }

        public void Complete()
        {
            _commit = true;
        }

        public static RabbitMQTransaction Begin()
        {
            var tx = new RabbitMQTransaction();
            _current = tx;
            return tx;
        }

        public void Add(IModel model)
        {
            _models.Add(model);
        }

        public void Enlist(Action<bool> completion)
        {            
            _completions += completion;
        }
    }
}