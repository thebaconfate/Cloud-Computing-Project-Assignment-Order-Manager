import Fastify from "fastify";
import mysql, { ResultSetHeader } from "mysql2/promise";
import * as dotenv from "dotenv";

dotenv.config();

interface NewOrder {
  user_id: number;
  timestamp_ns: number;
  price: number;
  symbol: string;
  quantity: number;
  order_type: string;
  trader_type: string;
}

const docker = false; // set to true to debug in a docker network, I don't think it's applicable to k8s though
const dbCredentials = {
  host: docker ? "host.docker.internal" : process.env.DB_HOST,
  user: process.env.DB_USER,
  port: process.env.DB_PORT ? parseInt(process.env.DB_PORT) : undefined,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
};

Object.entries(dbCredentials).some((credential) => {
  if (!credential[1]) throw new Error(`Undefined credential ${credential[0]}`);
});

const engineHost = "";
const enginePort = 0;
const enginePath = "";
const engineUrl = `${engineHost}:${enginePort}/${enginePath}`;

const marketDataPublisherHost = "";
const marketDataPublisherPort = 0;
const marketDataPublisherPath = "";
const marketDataPublisherUrl = `${marketDataPublisherHost}:${marketDataPublisherPort}/${marketDataPublisherPath}`;

const pool = mysql.createPool(dbCredentials);
pool.execute(
  "CREATE TABLE IF NOT EXISTS new_orders (" +
    [
      "secnum INT AUTO_INCREMENT PRIMARY KEY",
      "user_id INT NOT NULL",
      "timestamp_ns BIGINT NOT NULL",
      "price DECIMAL(65, 2) NOT NULL",
      "symbol VARCHAR(255) NOT NULL",
      "quantity INT NOT NULL",
      "order_type VARCHAR(255) NOT NULL",
      "trader_type VARCHAR(255) NOT NULL",
    ].join(", ") +
    ")",
);

async function insertOrder(newOrder: NewOrder) {
  const query =
    "INSERT INTO new_orders (user_id, timestamp_ns, price, symbol, quantity, order_type, trader_type) values (?, ?, ?, ?, ?, ?, ?)";
  return pool
    .execute<ResultSetHeader>(query, [
      newOrder.user_id,
      newOrder.timestamp_ns,
      newOrder.price,
      newOrder.symbol,
      newOrder.quantity,
      newOrder.order_type,
      newOrder.trader_type,
    ])
    .then((result) => {
      const [rows] = result;
      return rows.insertId;
    })
    .then((seqId) => {
      const seqOrder = {
        secnum: seqId,
        price: newOrder.price,
        quantity: newOrder.quantity,
        symbol: newOrder.symbol,
        side: newOrder.trader_type,
      };
      return seqOrder;
    });
}

const fastify = Fastify();

fastify.post<{ Body: string }>("/", async (request, replyTo) => {
  console.log("received order");
  const newOrder = JSON.parse(request.body) as NewOrder;
  insertOrder(newOrder)
    .then((id) => {
      console.log(id.secnum);
      replyTo.status(201).send();
    })
    .catch((e: any) => {
      console.error(e);
      replyTo.status(500).send();
    });
});

fastify.get("/", async (request, replyTo) => {
  replyTo.status(200).send("Order manager available");
});

fastify.listen({ port: 3000, host: "0.0.0.0" }, (err, addr) => {
  if (err) {
    console.error(err);
    process.exit(1);
  } else console.log(`Server listening on port: ${addr}`);
});
