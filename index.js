import aedes from "aedes";
import { createServer } from "net";
const port = 1889;

const aedesServer = aedes();
createServer(aedesServer.handle).listen(port, () => {
  console.log(`MQTT Broker running on port: ${port}`);
});

const users = [
  {
    id: null,
    username: "demolala",
    password: "demo123",
    topics: [],
  },
];

/**
 * FindClientById is a function that takes a client object as an argument and returns the user object
 * that matches the client's id
 * @param client - The client object that we want to find in the users array.
 */
const findClientById = (client) => users.find((el) => el.id === client.id);

/* A callback function that is called when a client tries to connect to the broker. */
aedesServer.authenticate = (client, username, password, callback) => {
  console.log(client);
  password = Buffer.from(password, "base64").toString();
  const user = users.find(
    (u) => u.username === username && u.password === password
  );
  if (user) {
    const index = users.findIndex((el) => el.username === username);
    users[index].id = client.id;
    return callback(null, true);
  }
  const error = new Error("Authentication Failed!! Invalid user credentials.");
  console.log("Error ! Authentication failed.");
  return callback(error, false);
};

/* This is the function that is called when a client publishes a message packet on the topic. */
aedesServer.authorizePublish = (client, packet, callback) => {
  const clientData = findClientById(client);
  if (clientData) {
    return callback(null);
  }
  console.log("Error ! Unauthorized publish to a topic.");
  return callback(
    new Error("You are not authorized to publish on this message topic.")
  );
};

/* This is the function that is called when a client subscribes to a topic. */
aedesServer.authorizeSubscribe = (client, sub, callback) => {
  const clientData = findClientById(client);
  if (clientData.topics.includes(sub.topic)) {
    return callback(null, sub);
  }
  console.log("Error ! Unauthorized subscribe to a topic.");
  return callback(
    new Error("You are not authorized to subscribe to this message topic.")
  );
};

// emitted when a client connects to the broker
aedesServer.on("client", function (client) {
  console.log(
    `[CLIENT_CONNECTED] Client ${
      client ? client.id : client
    } connected to broker ${aedesServer.id}`
  );
});

// emitted when a client disconnects from the broker
aedesServer.on("clientDisconnect", function (client) {
  console.log(
    `[CLIENT_DISCONNECTED] Client ${
      client ? client.id : client
    } disconnected from the broker ${aedesServer.id}`
  );
});

// emitted when a client subscribes to a message topic
aedesServer.on("subscribe", function (subscriptions, client) {
  console.log(
    `[TOPIC_SUBSCRIBED] Client ${
      client ? client.id : client
    } subscribed to topics: ${subscriptions
      .map((s) => s.topic)
      .join(",")} on broker ${aedesServer.id}`
  );
});

// emitted when a client unsubscribes from a message topic
aedesServer.on("unsubscribe", function (subscriptions, client) {
  console.log(
    `[TOPIC_UNSUBSCRIBED] Client ${
      client ? client.id : client
    } unsubscribed to topics: ${subscriptions.join(",")} from broker ${
      aedesServer.id
    }`
  );
});

// emitted when a client publishes a message packet on the topic
aedesServer.on("publish", async function (packet, client) {
  if (client) {
    console.log(
      `[MESSAGE_PUBLISHED] Client ${
        client ? client.id : "BROKER_" + aedesServer.id
      } has published message on ${packet.topic} to broker ${aedesServer.id}`
    );
    const clientData = findClientById(client);
    clientData.topics.push(packet.topic);
  }
});
