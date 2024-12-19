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

async function insertOrder(newOrder: NewOrder) {
  const query =
    "INSERT INTO orders (timestamp, price, symbol, quantity, quantity_left, side) values (?, ?, ?, ?, ?, ?)";
  return pool
    .execute<ResultSetHeader>(query, [
      new Date(),
      newOrder.price,
      newOrder.symbol,
      newOrder.quantity,
      newOrder.quantity,
      newOrder.order_type,
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
    })
    .catch((e: any) => {
      console.log(e);
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
