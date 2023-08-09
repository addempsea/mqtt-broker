import aedes from "aedes";
import { createServer } from "net";
import pgPromise from "pg-promise";
import promise from "bluebird";
import dotenv from 'dotenv';
import ws  from 'websocket-stream';
import { createServer as httpServer } from 'https';
import fs from 'fs';

dotenv.config();
const wsPort = 8883;
const port = 1889;
const currentDateTime = () => new Date();
const privateKey = fs.readFileSync('ssl-cert/privkey.pem', 'utf8');
const certificate = fs.readFileSync('ssl-cert/fullchain.pem', 'utf8');

const credentials = { key: privateKey, cert: certificate };
const httpServer2 = httpServer(credentials)

const aedesServer = aedes();
createServer(aedesServer.handle).listen(port, () => {
  console.log(`[${currentDateTime()}] MQTT Broker running on port: ${port}`);
});

ws.createServer({ server: httpServer2 }, aedesServer.handle)

httpServer2.listen(wsPort, function () {
  console.debug(`[${currentDateTime()}] Aedes MQTT-WS listening on port: ` + wsPort)
});

const pgp = pgPromise({ promiseLib: promise, noLocking: true });

const envs = {
  dbUrl: process.env.DB_URL,
}

const db = pgp(envs.dbUrl);
db.connect().then(() => {
  console.log('====================================');
  console.log('DB connected successfully');
  console.log('====================================');
});

/**
 * FindClientById is a function that takes a client object as an argument and returns the user object
 * that matches the client's id
 * @param client - The client object that we want to find in the users array.
 */
 const findById = async (id) =>  db.oneOrNone(`SELECT * FROM mqtt_user_info WHERE id = $1`, [id]);

/**
 * Save a log of who opened the door and who they let in.
 * @param openedBy - The name of the person who opened the door.
 * @param subscriberId - The ID of the subscriber who opened the door.
 */
const saveLog = async (openedBy, subscriberId) => db.none(`INSERT INTO door_log (opened_by, subscriber_id) VALUES ($1, $2)`, [openedBy, subscriberId]);

/**
 * Save data to the database.
 * @param data - The data that you want to save.
 * @param subscriberId - The id of the subscriber that sent the data
 * @param topic - The topic you want to subscribe to.
 */
const saveData = async (data, subscriberId, topic) => db.none(`INSERT INTO data (data, subscriber_id, topic) VALUES ($1, $2, $3)`, [data, subscriberId, topic]);


/* A callback function that is called when a client tries to connect to the broker. */
aedesServer.authenticate = (client, username, password, callback) => {
  if (!(client && client.id) || !username || !password) {
    const error = new Error("Authentication Failed!! Invalid client credentials.");
    console.log(`[${currentDateTime()}] Error ! Authentication failed. No client provided`);
    return callback(error, false);
  }
  const decryptedPassword = Buffer.from(password, "base64").toString();
  findById(client.id).then((user) => {
    if (user && user.username === username && user.password === decryptedPassword) {
      return callback(null, true);
    }
    const error = new Error("Authentication Failed!! Invalid user credentials.");
    console.log(`[${currentDateTime()}] Error ! Authentication failed.`);
    return callback(error, false);
  });
  
};

/* This is the function that is called when a client publishes a message packet on the topic. */
aedesServer.authorizePublish = (client, packet, callback) => {
  if (!(client && client.id)) {
    console.log(`Client: ${JSON.stringify(client, null, 2)}`);
    const error = new Error("Authentication Failed!! Invalid client credentials.");
    console.log(`[${currentDateTime()}] Error ! Authentication failed. No client provided`);
    return callback(error, false);
  }
  findById(client.id).then(async (user) => {
    if (user.topics) {
      const parsedTopics = user.topics.split(",");
      if (parsedTopics.includes(packet.topic)) {
        if(packet.topic === 'door/log') {
          await saveLog(packet.payload.toString(), user.subscriber_id);
        }
        await saveData(packet.payload.toString(), user.subscriber_id, packet.topic)
        return callback(null, packet);
      }
      console.log(`[${currentDateTime()}] Error ! Unauthorized publish to a topic.`);
      return callback(
        new Error("You are not authorized to publish to this message topic.")
      );
    }
  })
};

/* This is the function that is called when a client subscribes to a topic. */
aedesServer.authorizeSubscribe = (client, sub, callback) => {
  if (!(client && client.id)) {
    const error = new Error("Authentication Failed!! Invalid client credentials.");
    console.log(`[${currentDateTime()}] Error ! Authentication failed. No client provided`);
    return callback(error, false);
  }
  findById(client.id).then((user) => {
    if (user.topics) {
      const parsedTopics = user.topics.split(",");
      if (parsedTopics.includes(sub.topic)) {
        return callback(null, sub);
      }
      console.log(`[${currentDateTime()}] Error ! Unauthorized subscribe to a topic.`);
      return callback(
        new Error("You are not authorized to subscribe to this message topic.")
      );
    }
  })
};

// emitted when a client connects to the broker
aedesServer.on("client", function (client) {
  console.log(
    `[${currentDateTime()}] [CLIENT_CONNECTED] Client ${
      client ? client.id : client
    } connected to broker ${aedesServer.id}`
  );
});

// emitted when a client disconnects from the broker
aedesServer.on("clientDisconnect", function (client) {
  console.log(
    `[${currentDateTime()}] [CLIENT_DISCONNECTED] Client ${
      client ? client.id : client
    } disconnected from the broker ${aedesServer.id}`
  );
});

// emitted when a client subscribes to a message topic
aedesServer.on("subscribe", function (subscriptions, client) {
  console.log(
    `[${currentDateTime()}] [TOPIC_SUBSCRIBED] Client ${
      client ? client.id : client
    } subscribed to topics: ${subscriptions
      .map((s) => s.topic)
      .join(",")} on broker ${aedesServer.id}`
  );
});

// emitted when a client unsubscribes from a message topic
aedesServer.on("unsubscribe", function (subscriptions, client) {
  console.log(
    `[${currentDateTime()}] [TOPIC_UNSUBSCRIBED] Client ${
      client ? client.id : client
    } unsubscribed to topics: ${subscriptions.join(",")} from broker ${
      aedesServer.id
    }`
  );
});

// emitted when a client publishes a message packet on the topic
aedesServer.on("publish", async function (packet, client) {
  console.log('====================================');
  console.log(client);
  console.log('====================================');
  if (client) {
    console.log(
      `[${currentDateTime()}] [MESSAGE_PUBLISHED] Client ${
        client ? client.id : "BROKER_" + aedesServer.id
      } has published message on ${packet.topic} to broker ${aedesServer.id}`
    );
  }
  findById(client && client.id).then((user) => {
    if ([6].includes(user.subscriber_id)) {
      const dto = {
        location: packet.payload.toString(),
      }
    }
  })
});
