#!/usr/bin/env node

// rabbitmq implementation
const amqp = require("amqplib/callback_api");
const axios = require('axios');
const { time } = require("console");
const WebSocketServer = require('ws').Server;

const RMQ_USER = "covid-api-user";
const RMQ_PASSWORD = "9j%L*9U4CewmGZZ6u8zf";
const RMQ_HOST =
  "b-fccd3130-419b-4c98-b0d3-421707f92cbd.mq.sa-east-1.amazonaws.com";
const RMQ_PORT = "5671";

const COVID_API_URL = "https://if1015covidreports-api.herokuapp.com";

const QUEUE_GENERAL = "reports_queue_general";
const QUEUE_COUNTRIES = "reports_queue_countries";

let countriesData = {}
let brazilData = {}

const wss = new WebSocketServer({ port: process.env.PORT || "8080", path: '/requests' });

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

wss.on('connection', ws => {
    console.log('new connection');

    ws.send(JSON.stringify({ 
        countriesData: countriesData,
        brazilData: brazilData })
        );

    ws.on('close', (code, reason) => {
        console.log(`connection closed: ${code} - ${reason}`);
    });
});

const publishDataToCovidAPI = async (data, endpoint) => {
    await axios.post(`${COVID_API_URL}/${endpoint}`, data)
        .then(
            (response) => console.log("response: " + response.status),
            (error) => console.log("error: " + error),
        );
}

const consumeFromGeneralCasesQueue = (connectionChannel) => {
    connectionChannel.assertQueue(QUEUE_GENERAL, {durable: true});
    connectionChannel.prefetch(1)

    const onMessage = msg => {
        const messageJson = msg.content.toString();
        connectionChannel.ack(msg);
        brazilData = messageJson;
        publishDataToCovidAPI(JSON.parse(messageJson).data, 'reports/brazil');
        sleep(0.5 * 60 * 1000)
    };

    connectionChannel.consume(QUEUE_GENERAL, onMessage, { noAck: false });
}

const consumeFromCountriesQueue = (connectionChannel) => {
    connectionChannel.assertQueue(QUEUE_COUNTRIES, {durable: true});
    connectionChannel.prefetch(1)

    const onMessage = msg => {
        const messageJson = msg.content.toString();
        connectionChannel.ack(msg);
        countriesData = messageJson;
        publishDataToCovidAPI(JSON.parse(messageJson).data, 'reports/countries');
        sleep(0.5 * 60 * 1000)
    };

    connectionChannel.consume(QUEUE_COUNTRIES, onMessage, { noAck: false });
}

amqp.connect(`amqps://${RMQ_USER}:${RMQ_PASSWORD}@${RMQ_HOST}:${RMQ_PORT}`, (err, connection) => {
    if (err) throw err;

    connection.createChannel((err, channel) => {
      if (err) throw err;
      connectionChannel = channel;
      console.log('conectou')

      consumeFromGeneralCasesQueue(channel);
      consumeFromCountriesQueue(channel);
    });
  }
);