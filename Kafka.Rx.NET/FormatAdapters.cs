using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.RestClient;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET
{
    public static class FormatAdapters
    {
        /// <summary>
        /// This assumes that a topic has only one type.  Jay Kreps recommends separating different event types 
        /// into different topics at http://www.confluent.io/blog/stream-data-platform-2/.  Besides, heterogeneous-event 
        /// topics aren't directly supported by the driver.
        /// </summary>
        /// <returns></returns>
        public static async Task<Try<IEnumerable<Record<TK, TV>>>> ConsumeOnceAsAvroAsync<TK, TV>(
            IConfluentClient confluentClient,
            ConsumerInstance consumerInstance,
            string topic) where TK : class where TV : class

        {
            //exceptionTask.ContinueWith(result => Console.WriteLine("Faulted ..."), TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.AttachedToParent);
            //exceptionTask.ContinueWith(result => Console.WriteLine("Success ..."), TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.AttachedToParent);
            return await confluentClient.ConsumeAsAvroAsync<TK, TV>(consumerInstance, topic)
                .ContinueWith(task =>
                    new Success<IEnumerable<Record<TK, TV>>>(task.Result.Payload.Select(
                        record => (new Record<TK, TV>(record.Key, record.Value)))),
                    TaskContinuationOptions.OnlyOnRanToCompletion)
                .ContinueWith(task =>
                    new Failure<IEnumerable<Record<TK, TV>>>(task.Exception),
                    TaskContinuationOptions.OnlyOnFaulted);
            
        }
//
//        public static async Task<ConfluentResponse<List<AvroMessage<TK, TV>>>> ConsumeOnceAsJsonAsync<TK, TV>(
//            IConfluentClient confluentClient,
//            ConsumerInstance consumerInstance,
//            string topic)
//            where TK : class
//            where TV : class
//        {
//            var result = await confluentClient.ConsumeAsBinaryAsync<TK, TV>(consumerInstance, topic);
//            result.Payload[0]
//            return result;
//        }
    }
}
