using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.RestClient.Model;

namespace Kafka.Rx.NET.Console
{
    public class Program
    {
        static void Main(string[] args)
        {
            var options = new Options();

            if (CommandLine.Parser.Default.ParseArguments(args, options))
            {
                System.Console.WriteLine("Listening to " + options.BaseUrl);
                Listen(options);

            }
            else
            {
                System.Console.WriteLine(options.GetUsage());
            }
        }

        private static void Listen(Options options)
        {

            // Arrange
            String instanceId = options.InstanceId;
            string consumerGroupName = options.ConsumerGroup;
            string topic = options.Topic;
            ConsumerInstance consumerInstance = null;

            System.Console.WriteLine("Registering group/id: " + options.ConsumerGroup + "/" + options.InstanceId);
            System.Console.WriteLine("Listening to topic: " + topic);
            System.Console.WriteLine("Query interval: " + options.Sleep+"ms");
            using (var client = Setup.CreateConfluentClient(options.BaseUrl))
            {
                try
                {
                    // in production this should be written without blocking.
                    consumerInstance = Setup.ConsumerInstance(client, instanceId, consumerGroupName);
                    var consumer = new RxConsumer(client, consumerInstance, topic);

                    // Act
                    var observable = consumer.GetRecordStream<String, LogMessage>(
                        TimeSpan.FromMilliseconds(options.Sleep), ThreadPoolScheduler.Instance)
                        //.Take(10)
                        .Subscribe(successResult =>
                        {

                            System.Console.WriteLine("Success: " + successResult.IsSuccess);
                            if (successResult.IsSuccess)
                            {
                                System.Console.WriteLine(successResult.Value.Key + "=" +
                                                         successResult.Value.Value.Message);
                            }
                            else
                            {
                                System.Console.WriteLine("ERROR: " + successResult.Exception.Message);
                            }
                        },
                            () => System.Console.WriteLine("COMPLETED.") // not sure how to cancel this...
                        );

                    //Thread.Sleep(15000);
                    System.Console.ReadLine();
                    System.Console.WriteLine("Disposing observer");
                    observable.Dispose();
                }
                catch (Exception ex)
                {
                    System.Console.WriteLine(ex.Message);
                    if (ex.InnerException != null)
                    {
                        System.Console.WriteLine(ex.InnerException.Message);
                    }
                }
                finally
                {
                    if (consumerInstance != null)
                    {
                        System.Console.WriteLine("Destroying instance");
                        client.DeleteConsumerAsync(consumerInstance);
                    }
                }
            }
        }

    }
}
