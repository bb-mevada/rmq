// Dependencies
require("dotenv").config();
const uuid = require("uuid");
const express = require("express");
const amqplib = require("amqplib");

// Creating App Instance
const app = express();

// Middleware
app.use(express.json());

// Env Variables
const PORT = 5000;

// RabbitMQ Variables
let mqChannel = null;

// Routes
app.post("/message", (req, res) => {
    try {
        const { body } = req;
        const queueName = "Message";

        mqChannel.sendToQueue(
            queueName,
            Buffer.from(JSON.stringify({ _id: uuid.v4(), ...body })),
            {
                persistent: true,
            }
        );
        res.status(200).json({
            status: 200,
            message: "Success",
        });
    } catch (err) {
        res.status(500).json({
            status: 500,
            message: err.stack || err.message || "Internal Server Error",
        });
    }
});

// Connecting Rabbit MQ
const rabbitMQConnectionString = process.env.RABBIT_MQ_CONNECTION_STRING;

amqplib
    .connect(rabbitMQConnectionString)
    .then((connection) => {
        console.info(`Producer Connected With Rabbit MQ`);
        connection
            .createChannel()
            .then((channel) => {
                mqChannel = channel;
                const queueName = "Message";
                channel.assertQueue(queueName, { durable: true });
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

// Listening App
app.listen(PORT, () => {
    console.log(`Producer Application is running on port: ${PORT}`);
});
