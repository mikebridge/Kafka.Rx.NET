using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET
{
    /// <summary>
    /// Each RxConsumer is associated with an Consumer Instance for a particular Consumer Group.
    /// 
    /// The TV generic type refers to the ONE type that will be found within the topic.  Jay Kreps recommends separating 
    /// different event types into different topics at http://www.confluent.io/blog/stream-data-platform-2/,
    /// so this follows that advice.  Heterogeneous-event topics aren't directly supported by the driver
    /// anyway.
    /// </summary>
    /// <typeparam name="TK"></typeparam>
    /// <typeparam name="TV"></typeparam>
 

    
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

        /// </summary>
        /// <returns></returns>
        private static async Task<ConfluentResponse<List<AvroMessage<TK, TV>>>> ConsumeOnceAsync(
            IConfluentClient confluentClient,
            ConsumerInstance consumerInstance,
            string topic)
 
        {
            return await confluentClient.ConsumeAsAvroAsync<TK, TV>(consumerInstance, topic);
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
            return GetRecordStream(ConsumeOnceAsync, interval, scheduler, beforeCallAction);
        }



        public IObservable<Try<Record<TK, TV>>> GetRecordStream(
            Func<IConfluentClient, ConsumerInstance, String, Task<ConfluentResponse<List<AvroMessage<TK, TV>>>>> consumerAction,
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
                            SendResultToObserver(await consumerAction(_client, _consumerInstance, _topic), observer);
                            
                        }
                        catch (Exception ex)
                        {
                            // TODO: Write this to the observer via Try,
                            // but differentiate between fatal and non-fatal messages.
                            Console.WriteLine("EXCEPTION: " + ex.Message);
                            SendExceptionToObserver(ex, observer);
                        }
                        await scheduler.Sleep(interval, cancellationToken);
                    }

                });

            });
        }

        private static void SendExceptionToObserver<TK, TV>(
            Exception ex,
            IObserver<Try<Record<TK, TV>>> observer)
            where TK : class
            where TV : class
        {
            observer.OnNext(new Failure<Record<TK, TV>>(ex));
        }

        private static void SendResultToObserver<TK, TV>(
            ConfluentResponse<List<AvroMessage<TK, TV>>> result, 
            IObserver<Try<Record<TK, TV>>> observer) 
            where TK : class
            where TV : class
        {
            if (result.IsSuccess())
            {
                // flatten the result
                foreach (var record in result.Payload)
                {
                    Console.WriteLine("Sending "+record.Value+" to observer");
                    observer.OnNext(new Success<Record<TK, TV>>(new Record<TK, TV>(record.Key, record.Value)));
                }
            }
            else
            {
                Console.WriteLine("Got an error: " + result.Error.Message);
                observer.OnNext(new Failure<Record<TK, TV>>(
                        new Exception(result.Error.ErrorCode + ": " + result.Error.Message)));
            }
        }
    }
}

