import Fastify from "fastify";
import mysql, { ResultSetHeader } from "mysql2/promise";

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

async function insertOrder(newOrder: NewOrder) {
  const query =
    "INSERT INTO ORDERS (user_id, timestamp_ns, price, symbol, quantity, order_type, trader_type) values (?, ?, ?, ?, ?, ?)";
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
      const data = JSON.stringify(seqOrder);
      console.log(data);
    });
}

const fastify = Fastify();

fastify.post<{ Body: NewOrder }>("/", async (request, reply) => {
  const newOrder = request.body;
  insertOrder(newOrder);
});
