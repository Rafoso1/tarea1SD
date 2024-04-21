const express = require("express");
const axios = require("axios");
const { createClient } = require("redis");
const responseTime = require("response-time");
const { Client } = require('pg');
const crypto = require('crypto');
const app = express();

const client_redis_1 = createClient({
  host: "127.0.0.1",
  port: 6379,
});
const client_redis_2 = createClient({
  host: "127.0.0.1",
  port: 6380,
});
const client_redis_3 = createClient({
  host: "127.0.0.1",
  port: 6381,
});

const client_postgres = new Client({
  user: 'postgres',
  host: '172.29.0.2',
  database: 'bdd',
  password: 'postgres',
  port: 5432,
});

app.use(responseTime());

// Función hash MD5 para distribuir los datos entre las instancias de Redis
function hashToRedisInstance(input) {
  const hash = crypto.createHash('md5').update(input).digest('hex');
  const hashInt = parseInt(hash, 16);
  const instanceNumber = hashInt % 3; // Dividir por 3 para distribuir entre 3 instancias
  return instanceNumber;
}

app.get("/id/:id", async (req, res, next) => {
  try {
    const hrstart = process.hrtime();
    const instanceNumber = hashToRedisInstance(req.params.id);

    let reply;
    let data;

    switch (instanceNumber) {
      case 0:
        reply = await client_redis_1.get(req.params.id);
        break;
      case 1:
        reply = await client_redis_2.get(req.params.id);
        break;
      case 2:
        reply = await client_redis_3.get(req.params.id);
        break;
    }

    if (reply) {
      data = JSON.parse(reply);
      console.log(`Dato obtenido desde cache en instancia ${instanceNumber + 1}`);
    } else {
      const response = await client_postgres.query('SELECT * FROM tabla_urgencias WHERE id = $1', [req.params.id]);
      data = response.rows;
      await Promise.all([
        client_redis_1.set(req.params.id, JSON.stringify(data)),
        client_redis_2.set(req.params.id, JSON.stringify(data)),
        client_redis_3.set(req.params.id, JSON.stringify(data)),
      ]);
      console.log(`Dato guardado en caches, obtenido desde BDD y almacenado en instancia ${instanceNumber + 1}`);
    }

    const hrend = process.hrtime(hrstart);
    console.log(`Tiempo transcurrido: ${hrend[0]}s ${hrend[1] / 1000000}ms`);

    res.send(data);
  } catch (error) {
    console.log(error);
    res.send(error.message);
  }
});

app.get("/aire/:aire", async (req, res, next) => {
  try {
    const hrstart = process.hrtime(); // time start

    // Verificar si la respuesta ya está en alguna instancia de Redis
    const instanceNumber = hashToRedisInstance(req.params.aire);

    let reply;
    let data;

    switch (instanceNumber) {
      case 0:
        reply = await client_redis_1.get(req.params.aire);
        break;
      case 1:
        reply = await client_redis_2.get(req.params.aire);
        break;
      case 2:
        reply = await client_redis_3.get(req.params.aire);
        break;
    }

    if (reply) {
      data = JSON.parse(reply);
      console.log(`Dato obtenido desde cache en instancia ${instanceNumber + 1}`);
    } else {
      const response = await client_postgres.query('SELECT * FROM tabla_aire WHERE aire = $1', [req.params.aire]);
      data = response.rows;
      await Promise.all([
        client_redis_1.set(req.params.aire, JSON.stringify(data)),
        client_redis_2.set(req.params.aire, JSON.stringify(data)),
        client_redis_3.set(req.params.aire, JSON.stringify(data)),
      ]);
      console.log(`Dato guardado en caches, obtenido desde BDD y almacenado en instancia ${instanceNumber + 1}`);
    }

    const hrend = process.hrtime(hrstart); // time end
    console.log(`Tiempo transcurrido: ${hrend[0]}s ${hrend[1] / 1000000}ms`); // delta time

    res.send(data);
  } catch (error) {
    console.log(error);
    res.send(error.message);
  }
});


async function main() {
  try {
    await Promise.all([
      client_redis_1.connect(),
      client_redis_2.connect(),
      client_redis_3.connect(),
    ]);

    await client_postgres.connect();

    app.listen(3000);
    console.log("Server listening on port 3000");
  } catch (error) {
    console.error("Error during server startup:", error);
  }
}

main();
