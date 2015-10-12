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

        public static async Task<ConfluentResponse<ConsumerInstance>> ConsumerInstance(IConfluentClient client,
            string instanceId, string consumerGroupName)
        {

            var request = new CreateConsumerRequest
            {
                AutoCommitEnabled = true,
                // Confluent API will create a new InstanceId if not supplied
                InstanceId = instanceId,
                MessageFormat = MessageFormat.Avro
            };

            var consumerInstance = await client.CreateConsumerAsync(consumerGroupName, request);
            return consumerInstance;
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

        // TODO: Is this required?

        public class KafkaClientSettings : IConfluentClientSettings
        {
            public string KafkaBaseUrl { get; set; }
        }


        // TODO: Move this out of here

        public static IConfluentClient CreateConfluentClient(String baseUrl)
        {
            var settings = new KafkaClientSettings
            {
                KafkaBaseUrl = baseUrl
            };
            IConfluentClient client = new ConfluentClient(settings);
            return client;
        }

        /// <summary>
        /// Convert the set of records into an IObservable stream of individual
        /// records.  If an error occurs, Try.Failure will be returned, but the
        /// iobservable will continue to poll (hence the reason OnError is not called).
        /// 
        /// See: http://stackoverflow.com/questions/19547880/how-to-implement-polling-using-observables
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="interval"></param>
        /// <param name="scheduler"></param>
        /// <returns></returns>

        public IObservable<Try<Record<K, V>>> GetRecordStream<K, V>(
            TimeSpan interval,
            IScheduler scheduler)
            where K : class
            where V : class
        {
            return Observable.Create<Try<Record<K, V>>>(observer =>
            {
                return scheduler.ScheduleAsync(async (scheduler1, cancellationToken) =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine("Calling API");
                        var result = await ConsumeOnceAsync<K, V>(_client, _consumerInstance, _topic);
                        Console.WriteLine("  API Returned");
                        if (result.IsSuccess())
                        {
                            Console.WriteLine("Records returned: "+result.Payload.Count );
                            // flatten the result
                            foreach (var record in result.Payload)
                            {
                                Console.WriteLine("Telling observer about "+record.Value);
                                observer.OnNext(new Success<Record<K, V>>(new Record<K, V>(record.Key, record.Value)));
                            }
                        }
                        else
                        {
                            Console.WriteLine("TElling observer about error  " + result.Error.Message);
                            observer.OnNext(
                                new Failure<Record<K, V>>(
                                    new Exception(result.Error.ErrorCode + ": " + result.Error.Message)));
                        }
                    }
                    await scheduler1.Sleep(interval, cancellationToken);

                });

            });
        }
    }
}

