using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Rhino.ServiceBus.Transport;

namespace Rhino.ServiceBus.RabbitMQ
{
    [CLSCompliant(false)]
    public class RabbitMQTransaction : IRsbTransaction
    {
        [ThreadStatic] private static RabbitMQTransaction _current;
        private bool _commit;
        private Action<bool> _completions;

        private readonly ISet<IModel> _models = new HashSet<IModel>();

        public static RabbitMQTransaction Current
        {
            get { return _current; }
        }

        public void Dispose()
        {
            if (_completions != null)
            {
                foreach (Action<bool> action in _completions.GetInvocationList())
                    action(_commit);
                _completions = null;
            }

            _current = null;
            foreach (var model in _models)
            {
                if (_commit)
                    model.TxCommit();
                else
                    model.TxRollback();

                model.Dispose();
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