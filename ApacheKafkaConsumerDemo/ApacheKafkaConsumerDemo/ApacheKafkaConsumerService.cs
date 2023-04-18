using Confluent.Kafka;

namespace ApacheKafkaConsumerDemo
{
    public class ApacheKafkaConsumerService : IHostedService
    {
        private readonly string topic = "eticket";
        private readonly string groupId = "test_group";
        private readonly string bootstrapServers = "localhost:9092";

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                //   AutoOffsetReset = AutoOffsetReset.Earliest,
                //   EnableAutoCommit = false,
                //EnableAutoOffsetStore = false,
                // MaxPollIntervalMs = 300000,



                SecurityProtocol = SecurityProtocol.Plaintext,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };
            using var consumerBuilder = new ConsumerBuilder<long, string>(config)
              .SetKeyDeserializer(Deserializers.Int64)
              .SetValueDeserializer(Deserializers.Utf8)
              .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
              .Build();
            try
            {
                //using (var consumerBuilder = new ConsumerBuilder
                //<long, string>(config).Build())
                //{
                consumerBuilder.Subscribe(topic);
                Console.WriteLine("\nConsumer loop started...\n\n");
                // var cancelToken = new CancellationTokenSource();

                //try
                //{
                while (true)
                {
                    var result =
                consumerBuilder.Consume(
                    TimeSpan.FromMilliseconds(config.MaxPollIntervalMs - 1000 ?? 250000));
                    //var consumer = consumerBuilder.Consume
                    //   (cancelToken.Token);
                    //using (var writer = System.IO.File.CreateText(System.IO.Path.GetTempFileName()))
                    //{
                    //    writer.WriteLine("Consumeed Messsage" + DateTime.Now.Date + " ", consumer.Message.Value); //or .Write(), if you wish
                    //}
                    //var orderRequest = JsonSerializer.Deserialize
                    //    <SaveOrderOutput>
                    //        (consumer.Message.Value);
                    //Debug.WriteLine($"Processing Order Id:{consumer.Message.Value} ");

                    var message = result?.Message?.Value;
                    if (message == null)
                    {
                        continue;
                    }

                    Console.WriteLine(
                        $"Received: {result.Message.Key}:{message} from partition: {result.Partition.Value}");

                    consumerBuilder.Commit(result);
                    consumerBuilder.StoreOffset(result);
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }
                //  }
                //catch (OperationCanceledException)
                //{
                //    consumerBuilder.Close();
                //}
                //catch (KafkaException e)
                //{
                //    Console.WriteLine($"Consume error: {e.Message}");
                //    Console.WriteLine("Exiting producer...");
                //}
                //finally
                //{
                //    consumerBuilder.Close();
                //}

            }
            //catch (Exception ex)
            //{
            //    System.Diagnostics.Debug.WriteLine(ex.Message);
            //}
            catch (KafkaException e)
            {
                Console.WriteLine($"Consume error: {e.Message}");
                Console.WriteLine("Exiting producer...");
            }
            finally
            {
                consumerBuilder.Close();
            }
            return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}

