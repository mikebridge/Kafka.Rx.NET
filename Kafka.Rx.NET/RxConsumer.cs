using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET
{
    // each RxConsumer is associated with an Consumer Instance for a particualr Consumer Group.
    public class RxConsumer
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
       

        private async Task<ConfluentResponse<List<AvroMessage<K, V>>>> ConsumeOnceAsync<K, V>(
            IConfluentClient confluentClient,
            ConsumerInstance consumerInstance,
            string topic)
            where K : class
            where V : class
        {
            return await confluentClient.ConsumeAsAvroAsync<K, V>(consumerInstance, topic);
        }

        /// <summary>
        /// Convert the set of records into an IObservable stream of individual
        /// records.  If an error occurs, Try.Failure will be returned, but the
        /// iobservable will continue to poll (hence the reason OnError is not called).
        /// 
        /// See: http://stackoverflow.com/questions/19547880/how-to-implement-polling-using-observables
        /// </summary>
        /// <returns></returns>
        public IObservable<Try<Record<K, V>>> GetRecordStream<K, V>(
            TimeSpan interval,
            IScheduler scheduler)
            where K : class
            where V : class
        {
            return GetRecordStream(ConsumeOnceAsync<K, V>, interval, scheduler);
        }



        public IObservable<Try<Record<K, V>>> GetRecordStream<K, V>(
            Func<IConfluentClient, ConsumerInstance, String, Task<ConfluentResponse<List<AvroMessage<K, V>>>>> consumerAction,
            TimeSpan interval,
            IScheduler scheduler)
            where K : class
            where V : class
        {
            return Observable.Create<Try<Record<K, V>>>(observer =>
            {
                Console.WriteLine("Creating Observable");
                return scheduler.ScheduleAsync(async (scheduler1, cancellationToken) =>
                {
                    Console.WriteLine("Schedule Async");
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            Console.WriteLine("Calling action");
                            var result = await consumerAction(_client, _consumerInstance, _topic);
                            // TODO: check for exception
                            SendResultToObserver(result, observer);
                            
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("EXCEPTION!! " + ex.Message);
                        }
                        await scheduler.Sleep(interval, cancellationToken);
                    }
                   
                    

                });

            });
        }

        private static void SendResultToObserver<K, V>(
            ConfluentResponse<List<AvroMessage<K, V>>> result, 
            IObserver<Try<Record<K, V>>> observer) 
            where K : class
            where V : class
        {
            if (result.IsSuccess())
            {
                // flatten the result
                foreach (var record in result.Payload)
                {
                    Console.WriteLine("Sending "+record.Value+" to observer");
                    observer.OnNext(new Success<Record<K, V>>(new Record<K, V>(record.Key, record.Value)));
                }
            }
            else
            {
                Console.WriteLine("Got an error: " + result.Error.Message);
                observer.OnNext(
                    new Failure<Record<K, V>>(
                        new Exception(result.Error.ErrorCode + ": " + result.Error.Message)));
            }
        }
    }
}

