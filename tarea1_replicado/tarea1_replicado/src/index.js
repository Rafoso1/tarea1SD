const express = require("express");
const axios = require("axios");
const { createClient } = require("redis");
const responseTime = require("response-time"); //middleware de express
const { Client } = require('pg');
const app = express();

//   http://localhost:3000/id/PLSSA000834
//   http://localhost:3000/id/MSSSA000961
//   http://localhost:3000/aire/Nitrogen dioxide (NO2)
//   http://localhost:3000/aire/Ozone (O3)

// CONFIG SET maxmemory-policy volatile-lfu
// CONFIG SET maxmemory-policy volatile-lru


// Conectando a Redis
const client_redis_1 = createClient({
  host: "127.0.0.1", // localhost
  port: 6379, // Puerto diferente para la segunda instancia
});
const client_redis_2 = createClient({
  host: "127.0.0.1",
  port: 6380, // Puerto diferente para la segunda instancia
});
const client_redis_3 = createClient({
  host: "127.0.0.1",
  port: 6381, // Puerto diferente para la tercera instancia
});

// Conectando a Postgres
const client_postgres = new Client({
  user: 'postgres',
  host: '172.29.0.2',
  database: 'bdd',
  password: 'postgres',
  port: 5432,
});

app.use(responseTime());


// Obtener solo un id en espcifico
app.get("/id/:id", async (req, res, next) => {
  try {
    // Verificar si la respuesta ya está en alguna instancia de Redis
    const hrstart = process.hrtime(); // time start

    const reply_1 = await client_redis_1.get(req.params.id);
    const reply_2 = await client_redis_2.get(req.params.id);
    const reply_3 = await client_redis_3.get(req.params.id);

    let data;

    if (reply_1) {
      data = JSON.parse(reply_1);
      console.log("Dato obtenido desde cache");
    } else if (reply_2) {
      data = JSON.parse(reply_2);
      console.log("Dato obtenido desde cache");
    } else if (reply_3) {
      data = JSON.parse(reply_3);
      console.log("Dato obtenido desde cache");
    } else {
      // obtener data desde BDD y guardar en todos los Redis
      const response = await client_postgres.query('SELECT * FROM tabla_urgencias WHERE id = $1', [req.params.id]);
      data = response.rows;
      await Promise.all([
        client_redis_1.set(req.params.id, JSON.stringify(data)),
        client_redis_2.set(req.params.id, JSON.stringify(data)),
        client_redis_3.set(req.params.id, JSON.stringify(data)),
      ]);
      console.log("Dato guardado en cache, obtenido desde BDD");
    }

    const hrend = process.hrtime(hrstart); // time end
    console.log(`Tiempo transcurrido: ${hrend[0]}s ${hrend[1] / 1000000}ms`); // delta time

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
    const reply_1 = await client_redis_1.get(req.params.aire);
    const reply_2 = await client_redis_2.get(req.params.aire);
    const reply_3 = await client_redis_3.get(req.params.aire);

    let data;

    if (reply_1) {
      data = JSON.parse(reply_1);
      console.log("Dato obtenido desde cache");
    } else if (reply_2) {
      data = JSON.parse(reply_2);
      console.log("Dato obtenido desde cache");
    } else if (reply_3) {
      data = JSON.parse(reply_3);
      console.log("Dato obtenido desde cache");
    } else {
      // Si no está en ninguna instancia, obtener de la base de datos y guardar en todas las instancias
      const response = await client_postgres.query('SELECT * FROM tabla_aire WHERE aire = $1', [req.params.aire]);
      data = response.rows;
      await Promise.all([
        client_redis_1.set(req.params.aire, JSON.stringify(data)),
        client_redis_2.set(req.params.aire, JSON.stringify(data)),
        client_redis_3.set(req.params.aire, JSON.stringify(data)),
      ]);
      console.log("Dato guardado en cache, obtenido desde BDD");
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
    // Conectarse a todas las instancias de Redis
    await Promise.all([
      client_redis_1.connect(),
      client_redis_2.connect(),
      client_redis_3.connect(),
    ]);

    // Conectarse a PostgreSQL
    await client_postgres.connect();

    // Iniciar el servidor
    app.listen(3000);
    console.log("Server listening on port 3000");
  } catch (error) {
    console.error("Error during server startup:", error);
  }
}

main();
