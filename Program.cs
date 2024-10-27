using System.Text;
using EasyNetQ;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;

await using var container = await LaunchRabbitMqTestcontainer();
using var connection = CreateConnection(container);
using var model = connection.CreateModel();


// DISCLAIMER: copilot may decrease the efficiency of the excersize, so use at your own risk
// If you decide to use copilot, make sure to understand the code that it generates.
// If you do not understand the code, dive into documentation and code examples, look at the RabbitMQ UI, ask questions until you finally grasp the topic.
//
// Useful links:
// - https://www.rabbitmq.com/docs (publishing section for exchanges, queues section for queues, etc.)
// - https://www.rabbitmq.com/tutorials/tutorial-one-dotnet (code examples for .NET)
//
// RabbitMQ is launched at the start of this console app and closed at the end
// It is expected that Docker is running on your machine.
// So do NOT close the app until you are finished looking at the RabbitMQ management interface (http://localhost:15672/)
// For these excersizes we will use a lower-level API of RabbitMQ, which is the .NET client library
// Even if you usually use EasyNetQ, all of the concepts that we will cover are still used in that library under the hood

#region Excercize 1. Queues and Exchanges
// RabbitMQ introduces the concept of exchanges and queues.
// It is important to note that RabbitMQ introduces only one way to publish messages - to an exchange.
// You can not publish a message to a queue directly, an exchange is always involved.

// 1.1. TODO: Create an exchange named "my_exchange" of type "direct"
//
// Take a look at the RabbitMQ management interface to see if the exchange was created
// HINT: go to http://localhost:15672/ and login with guest/guest
model.ExchangeDeclare("my_exchange", ExchangeType.Direct);

// As mentioned before, you can publish a message to an exchange, so let's do that
// 1.2. TODO: Publish a message with the text "Hello World" to the exchange "my_exchange"
// Use no routing key for now
//
// Take a look at the statistics of this exchange in the Rabbit UI
// Take a look at the queues tab in the Rabbit UI, do you see any queues with messages?
model.BasicPublish("my_exchange", "", null, Encoding.UTF8.GetBytes("Hello World"));

// When we use messaging systems, we want messages to be delivered to a queue, so that we can handle it.
// 1.3. TODO: Create a queue named "my_queue"
//
// Take a look at the queues tab in the Rabbit UI, do you see the queue?
model.QueueDeclare("my_queue", false, false, false, null);


// Now, we want to bind the exchange to the queue, so that messages are delivered to the queue
// 1.4. TODO: Bind the queue "my_queue" to the exchange "my_exchange" with the routing key "#"
//
// Take a look at the bindings tab in the queue and exchange page in the Rabbit UI, do you see the binding?
model.QueueBind("my_queue", "my_exchange", "#");

// Finally, we want to publish the message again, and it should now be delivered to the queue
// 1.5. TODO: Publish a message with the text "Hello World" to the exchange "my_exchange"
// HINT: use Encoding.UTF8.GetBytes to encode the string to a byte array. RabbitMQ only accepts byte arrays as message bodies.
//
// Take a look at the queue page in the Rabbit UI, do you see the message in the queue?
// Try pressing the read all messages button, can you find the hello world message?
model.BasicPublish("my_exchange", "", null, Encoding.UTF8.GetBytes("Hello World"));

// We now went to the whole flow of the publishing a message to a queue. 
// You might wonder, how do some libraries implement direct queue sending if RabbitMQ does not support that?
// Fortunately, RabbitMQ has a feature called default exchange, which is a exchange with an empty name.
// Whenever you publish a message to that exchange with a routing key that matches the queue name, the message is delivered to the queue.

// 1.6. TODO: Publish a message with the text "Hello World" to the exchange "" (empty string) and routing key "my_queue"
//
// Take a look at the queue page in the Rabbit UI, do you see the message in the queue?
// Take a look at the my_exchange statistics in the Rabbit UI, do you see any messages passed through it?
model.BasicPublish("", "my_queue", null, Encoding.UTF8.GetBytes("Hello World"));

// 1.7. RabbitMQ has various types of exchanges and they route messages to queus in different ways.
// Let's create an exchange my_fanout_exchange of type "fanout"
// Create two queues my_fanout_queue1 and my_fanout_queue2
// Bind both queues to the exchange
// Publish a message to the exchange
//
// Take a look at the queues and exchange in the Rabbit UI, where was the message delivered?
model.ExchangeDeclare("my_fanout_exchange", ExchangeType.Fanout);
model.QueueDeclare("my_fanout_queue1", false, false, false, null);
model.QueueDeclare("my_fanout_queue2", false, false, false, null);
model.QueueBind("my_fanout_queue1", "my_fanout_exchange", "");
model.QueueBind("my_fanout_queue2", "my_fanout_exchange", "");
model.BasicPublish("my_fanout_exchange", "", null, Encoding.UTF8.GetBytes("Hello World"));

// RabbitMQ also has a type of exchange called "topic", which can filter the messages based on the routing key.
// 1.8. TODO: Create an exchange my_topic_exchange of type "topic"
// Create two queues my_topic_queue1 and my_topic_queue2
// Bind my_topic_queue1 to the exchange with the routing key "topic1"
// Bind my_topic_queue2 to the exchange with the routing key "topic2"
// Publish a message to the exchange with the routing key "topic1"
//
// Take a look at the queues and exchange in the Rabbit UI, where was the message delivered?
model.ExchangeDeclare("my_topic_exchange", ExchangeType.Topic);
model.QueueDeclare("my_topic_queue1", false, false, false, null);
model.QueueDeclare("my_topic_queue2", false, false, false, null);
model.QueueBind("my_topic_queue1", "my_topic_exchange", "topic1");
model.QueueBind("my_topic_queue2", "my_topic_exchange", "topic2");
model.BasicPublish("my_topic_exchange", "topic1", null, Encoding.UTF8.GetBytes("Hello World"));

// 1.9. Final excersize
// TODO: Implement a method with the following signature: 
// void PublishMessage<T>(IModel model, T message);
//
// Implement the method in such a way, that it handles creation of the exchange and queue, and binding them together.
// The method should publish the message to a separate exchange for each type of message(T).
// Each type of message(T) should be routed to a separate queue.

void PublishMessage<T>(IModel model, T message)
{
	var typeName = typeof(T).Name;
	var exchangeName = $"my_exchange_{typeName}";
	var queueName = $"my_queue_{typeName}";
	model.ExchangeDeclare(exchangeName, ExchangeType.Direct);
	model.QueueDeclare(queueName, false, false, false, null);
	model.QueueBind(queueName, exchangeName, "");
	model.BasicPublish(exchangeName, "", null, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message)));
}

PublishMessage(model, new { Message = "Hello World" });
PublishMessage(model, "hehe");
PublishMessage(model, 123);

#endregion

#region Excercize 2. Consumers
// Now that we have a message in the queue, we want to do something with it.
// There are multiple ways to consume messages from a queue, but we will use the most simple method - just getting the message from the queue.
// Task 2.1 Use BasicGet to get the message from the queue (set autoAck to false). 
// Create and exchange my_consuming_exchange and bind it to a queue my_consuming_queue
// Publish a message to the exchange
// Get the message from the queue
// Output the message to the console
// HINT: Use Encoding.UTF8.GetString to convert the byte array to a string, so that you can output it to the console
//
// Do you see the correct message in the console?
// Take a look at the queue page in the Rabbit UI, do you see the message removed from the queue?
// Try setting the autoAck parameter to true, does something change?
model.ExchangeDeclare("my_consuming_exchange", ExchangeType.Direct);
model.QueueDeclare("my_consuming_queue", false, false, false, null);
model.QueueBind("my_consuming_queue", "my_consuming_exchange", "");
model.BasicPublish("my_consuming_exchange", "", null, Encoding.UTF8.GetBytes("Hello World"));
var message = model.BasicGet("my_consuming_queue", false);
Console.WriteLine(Encoding.UTF8.GetString(message.Body.ToArray()));

// An interesting thing you will see in the statistics tab is that the message is now shows as 'unacked'. 
// This means that if your app shuts down, the message will be redelivered to the queue (basically, guaranteeing that it will be handled if the app restarts from time to time).
// And the total number of messages in the queue did not decrease. That changed when we set autoAck to true.
// Usually, we send an acknowledge signal when we handled the message, to indicate that RabbitMQ can now delete it.
// So let's do that.
// Task 2.2. Acknowledge the message that you retrieved from the queue
// HINT: Use BasicAck and the DeliveryTag (basically, the consumed message identifier) from the message
//
// Take a look at the queue page in the Rabbit UI, do you see the message removed from the queue?
model.BasicAck(message.DeliveryTag, false);

#endregion




Console.WriteLine("Press enter to exit");
Console.ReadLine();

static async Task<RabbitMqContainer> LaunchRabbitMqTestcontainer()
{
    var rabbitMqContainer = new RabbitMqBuilder()
        .WithImage("rabbitmq:3-management")
        .WithPortBinding(15672, 15672)
        .WithPortBinding(5672, assignRandomHostPort: true)
        .WithUsername("guest")
        .WithPassword("guest")
        .Build();

    await rabbitMqContainer.StartAsync();

    return rabbitMqContainer;
}

static IConnection CreateConnection(RabbitMqContainer container)
{
    var connectionFactory = new ConnectionFactory
    {
        Uri = new($"amqp://guest:guest@localhost:{container.GetMappedPublicPort(5672)}")
    };

    return connectionFactory.CreateConnection();
}
