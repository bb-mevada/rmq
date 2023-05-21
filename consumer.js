require("dotenv").config();
const express = require("express");
const amqplib = require("amqplib");

const app = express();
app.use(express.json());

const PORT = 5001;

// Connecting Rabbit MQ
const rabbitMQConnectionString = process.env.RABBIT_MQ_CONNECTION_STRING;

const retryLimit = 3; // Maximum number of retries
const retryDelay = 5000; // Delay in milliseconds between retries

const retryMessage = (channel, message) => {
    try {
        const retries = parseInt(
            message.properties.headers["x-retries"] || "0"
        );

        if (retries < retryLimit) {
            // Increment the retry count and add a delay before retrying
            const nextRetry = retries + 1;
            const delay = nextRetry * retryDelay;

            setTimeout(() => {
                try {
                    const headers = Object.assign(
                        {},
                        message.properties.headers,
                        {
                            "x-retries": nextRetry.toString(),
                        }
                    );

                    channel.publish(
                        "",
                        message.fields.routingKey,
                        message.content,
                        { headers }
                    );

                    // Reject the original message
                    channel.reject(message, false);
                } catch (err) {
                    console.error("Error occurred during message retry:", err);
                }
            }, delay);
        } else {
            // Retry limit reached, discard the message
            console.info("Retry limit reached, giving up on message");
            channel.reject(message, false);
        }
    } catch (err) {
        console.error("Error occurred during message retry:", err);
    }
};

amqplib
    .connect(rabbitMQConnectionString)
    .then((connection) => {
        console.info(`Consumer Connected With Rabbit MQ`);
        connection
            .createChannel()
            .then((channel) => {
                const queueName = "Message";
                channel.assertQueue(queueName, { durable: true });

                channel.consume(queueName, (message) => {
                    const msgContent = JSON.parse(message.content.toString());
                    console.log(msgContent);
                    try {
                        a;
                        channel.ack(message);
                    } catch (err) {
                        retryMessage(channel, message);
                    }
                });
            })
            .catch((err) => {
                console.error(err.stack || err.message);
                process.exit(0);
            });
    })
    .catch((err) => {
        console.error(err.stack || err.message);
        process.exit(0);
    });

app.listen(PORT, () => {
    console.log(`Consumer Application is running on port: ${PORT}`);
});
