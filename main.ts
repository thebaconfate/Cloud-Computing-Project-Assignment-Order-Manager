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

const dbCredentials = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  port: process.env.DB_PORT ? parseInt(process.env.DB_PORT) : undefined,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
};

Object.entries(dbCredentials).some((credential) => {
  if (!credential[1]) throw new Error(`Undefined credential ${credential[0]}`);
});

const engineHost = "matching-engine";
const enginePort = 3000;
const enginePath = "order";
const engineUrl = `http://${engineHost}:${enginePort}/${enginePath}`;

const marketDataPublisherHost = "market-data-publisher";
const marketDataPublisherPort = 3001;
const marketDataPublisherUrl = (marketDataPublisherPath: string) =>
  `http://${marketDataPublisherHost}:${marketDataPublisherPort}/${marketDataPublisherPath}`;

const pool = mysql.createPool(dbCredentials);
pool
  .execute(
    "CREATE TABLE IF NOT EXISTS orders (" +
      [
        "secnum INT AUTO_INCREMENT PRIMARY KEY",
        "user_id INT NOT NULL",
        "timestamp_ns BIGINT NOT NULL",
        "price DECIMAL(65, 2) NOT NULL",
        "symbol VARCHAR(255) NOT NULL",
        "quantity INT NOT NULL",
        "side VARCHAR(255) NOT NULL",
        "trader_type VARCHAR(255) NOT NULL",
        "filled BOOLEAN NOT NULL DEFAULT FALSE",
      ].join(", ") +
      ")",
  )
  .then(() => {
    pool.execute(
      "CREATE TABLE IF NOT EXISTS executions (" +
        [
          "secnum INT NOT NULL",
          "quantity INT NOT NULL",
          "FOREIGN KEY (secnum) REFERENCES orders(secnum)",
        ].join(", ") +
        ")",
    );
  });

async function insertOrder(newOrder: NewOrder) {
  const query =
    "INSERT INTO orders (user_id, timestamp_ns, price, symbol, quantity, side, trader_type) values (?, ?, ?, ?, ?, ?, ?)";
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
        side: newOrder.order_type,
      };
      return seqOrder;
    });
}

const fastify = Fastify();

fastify.post("/order", async (request, reply) => {
  const newOrder = request.body as unknown as NewOrder;
  try {
    const order = await insertOrder(newOrder);
    const stringifiedOrder = JSON.stringify(order);
    Promise.allSettled([
      fetch(engineUrl, {
        method: "POST",
        body: stringifiedOrder,
        headers: { "Content-Type": "application/json" },
      }),
      fetch(marketDataPublisherUrl("order"), {
        method: "POST",
        body: stringifiedOrder,
        headers: { "Content-Type": "application/json" },
      }),
    ]);
    reply.code(201).send();
  } catch (e: any) {
    console.error(e);
  }
});

fastify.post("/order-fill", async (request, _) => {
  try {
    await fetch(marketDataPublisherUrl("executions"), {
      method: "POST",
      body: JSON.stringify(request.body),
      headers: { "Content-Type": "application/json" },
    });
  } catch (e: any) {
    console.error(e);
  }
});

fastify.get("/", async (_, replyTo) => {
  replyTo.code(200).send("Order manager available");
});

fastify.listen({ port: 3000, host: "0.0.0.0" }, (err, addr) => {
  if (err) {
    console.error(err);
    process.exit(1);
  } else {
    console.log(`Server listening on port: ${addr}`);
  }
});
