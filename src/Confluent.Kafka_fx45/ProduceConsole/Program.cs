using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProduceConsole
{
    class Program
    {
        static string brokerList = "localhost:9092";
        static string topicName = "nulltopic";

        static void Main(string[] args)
        {
            RunSimpleProducer();
        }

        static void RunSimpleProducer()
        {
            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList }, { "topic.metadata.refresh.interval.ms", 1000 }, { "metadata.max.age.ms", 5000 }, { "default.topic.config", new Dictionary<string, object> { { "acks", -1 } } } };

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");
                producer.OnError += Producer_OnError;
                producer.OnLog += Producer_OnLog;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    var deliveryReport = producer.ProduceAsync(topicName, null, text);
                    deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }

                // Tasks are not waited on synchronously (ContinueWith is not synchronous),
                // so it's possible they may still in progress here.
                producer.Flush();
            }
        }

        private static void Producer_OnLog(object sender, LogMessage e)
        {
            Console.WriteLine($"Producer_OnLog, {e.Level}, {e.Name}, {e.Message}, {e.Facility}");
        }

        private static void Producer_OnError(object sender, Error e)
        {
            Console.WriteLine($"Producer_OnError, {e.Code}, {e.Reason}");
        }
    }
}
