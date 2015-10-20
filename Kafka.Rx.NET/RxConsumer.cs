using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET
{
    // each RxConsumer is associated with an Consumer Instance for a particualr Consumer Group.
    public class RxConsumer<TK, TV>
        where TK : class
        where TV : class
    {
        private readonly ConsumerInstance _consumerInstance;

        private readonly string _topic;

        private readonly IConfluentClient _client;

        //private readonly OffsetCommitter _committer;

        public RxConsumer(IConfluentClient client, ConsumerInstance consumerInstance, String topic)
        {
            _client = client;
            _consumerInstance = consumerInstance;
            _topic = topic;
        }

        /// <summary>
        /// Convert the set of records into an IObservable stream of individual
        /// records.  If an error occurs, Try.Failure will be returned, but the
        /// iobservable will continue to poll (hence the reason OnError is not called).
        /// 
        /// See: http://stackoverflow.com/questions/19547880/how-to-implement-polling-using-observables
        /// </summary>
        /// <returns></returns>
        public IObservable<Try<Record<TK, TV>>> GetRecordStream(
            TimeSpan interval,
            IScheduler scheduler,
            Action beforeCallAction = null
            )
        {
            return GetRecordStream(FormatAdapters.ConsumeOnceAsAvroAsync<TK,TV>, interval, scheduler, beforeCallAction);
        }



        public IObservable<Try<Record<TK, TV>>> GetRecordStream(
            Func<IConfluentClient, ConsumerInstance, String, Task<Try<IEnumerable<Record<TK, TV>>>>> consumerAction,
            TimeSpan interval,
            IScheduler scheduler,
            Action beforeCallAction = null)
        {
            return Observable.Create<Try<Record<TK, TV>>>(observer =>
            {
                beforeCallAction = beforeCallAction ?? (() => { });

                return scheduler.ScheduleAsync(async (scheduler1, cancellationToken) =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        beforeCallAction();
                        try
                        {
                          var result = await consumerAction(_client, _consumerInstance, _topic);
                            if (result.IsSuccess)
                            {
                                // flatten the result
                                foreach (var record in result.Value.ToList())
                                {
                                    //Console.WriteLine("Sending "+record.Value+" to observer");
                                    observer.OnNext(new Success<Record<TK, TV>>(record));
                                }
                            }
                            else
                            {
                                //Console.WriteLine("Got an error: " + result.Exception.Message);
                                observer.OnNext(new Failure<Record<TK, TV>>(result.Exception));

                            }
                        }
                        catch (Exception ex)
                        {
                            // TODO: Write this to the observer via Try,
                            // but differentiate between fatal and non-fatal messages.
                            //Console.WriteLine("EXCEPTION: " + ex.Message);
                            SendExceptionToObserver(ex, observer);
                        }
                        await scheduler.Sleep(interval, cancellationToken);
                    }

                });

            });
        }

        private static void SendExceptionToObserver(
            Exception ex,
            IObserver<Try<Record<TK, TV>>> observer)
        {
            observer.OnNext(new Failure<Record<TK, TV>>(ex));
        }
    }
}

