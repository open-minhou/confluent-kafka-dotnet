using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosumerConsole
{
    class Program
    {
        static string groupId = "consumergroup123";
        static string brokerList = "localhost:9093,localhost:9094";
        static string topic = "nulltopic";
        static int[] partitions = new int[] { 0,1,2,3,4 };
        static void Main(string[] args)
        {
            AdvancedConsumer.Run_Poll(brokerList, new List<string> { topic });
            //RunSimpleConsumer();
        }

        static void RunSimpleConsumer()
        {
            var config = new Dictionary<string, object>
            {
                { "group.id", groupId },
                { "bootstrap.servers", brokerList }
            };

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                List<TopicPartitionOffset> topicToAssign = new List<TopicPartitionOffset>();
                foreach (int partition in partitions)
                {
                    topicToAssign.Add(new TopicPartitionOffset(topic, partition, Offset.Beginning));
                }
                consumer.Assign(topicToAssign);

                while (true)
                {
                    Message<Null, string> msg;
                    if (consumer.Consume(out msg))
                    {
                        Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                    }
                }
            }
        }
    }
}
