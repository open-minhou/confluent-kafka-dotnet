using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosumerConsole
{
    public class AdvancedConsumer
    {

        private static Dictionary<string, object> constructConfig(string brokerList, bool enableAutoCommit) =>
    new Dictionary<string, object>
    {
                { "group.id", "consumergroup9" },
                { "enable.auto.commit", enableAutoCommit },
                { "auto.commit.interval.ms", 5000 },
                //{ "statistics.interval.ms", 60000 },
                { "bootstrap.servers", brokerList },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
    };

        public static void Run_Poll(string brokerList, List<string> topics)
        {
            using (var consumer = new Consumer<Null, string>(constructConfig(brokerList, true), null, new StringDeserializer(Encoding.UTF8)))
            {
                // Note: All event handlers are called on the main thread.

                consumer.OnMessage += (_, msg)
                    => Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

                consumer.OnError += (_, error)
                    => Console.WriteLine($"Error: {error}");

                consumer.OnOffsetsCommitted += (_, commit) =>
                {
                    Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

                    if (commit.Error)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                };

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    consumer.Assign(partitions);
                };

                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.Unassign();
                };

                consumer.OnStatistics += (_, json)
                    => Console.WriteLine($"Statistics: {json}");

                consumer.Subscribe(topics);

                Console.WriteLine($"Subscribed to: [{string.Join(", ", consumer.Subscription)}]");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C to exit.");
                while (!cancelled)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }
    }
}
