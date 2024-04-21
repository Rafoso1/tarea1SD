const express = require("express");
const axios = require("axios");
const { createClient } = require("redis");
const responseTime = require("response-time"); //middleware de express
const { Client } = require('pg');
const app = express();

// Conectando a Redis
const client_redis = createClient({
  host: "127.0.0.1", // localhost
  port: 6379,
});

// Conectando a Postgres
const client_postgres = new Client({
  user: 'postgres',
  host: '172.27.0.2',
  database: 'clinica',
  password: 'postgres',
  port: 5432,
});

app.use(responseTime());

// Obtener todos los datos
app.get("/id", async (req, res, next) => {
  try {
    
    const hrstart = process.hrtime(); // time start
    
    // Verificar si la respuesta ya esta en Redis
    const reply = await client_redis.get("id");
    
    if (reply) {
      const hrend = process.hrtime(hrstart); // time end
      console.log("Data obtenida desde cache");
      console.log(`Tiempo transcurrido: ${hrend[0]}s ${hrend[1] / 1000000}ms`); // delta time
      console.log("Data count: " + JSON.parse(reply).length);
      return res.send(JSON.parse(reply));
    }

    // Obtener info desde base de datos
    const response = await client_postgres.query('SELECT * FROM urgencias LIMIT 1000000');

    const hrend = process.hrtime(hrstart); // time end
    console.log(`Tiempo transcurrido: ${hrend[0]}s ${hrend[1] / 1000000}ms`); // delta time
    // Guardar resultado en Redis con .set
    const saveInRedis = await client_redis.set("id",JSON.stringify(response.rows));
    console.log("nueva data almacenada en cache:" + saveInRedis);

    // reenviar al cliente
    // response.push(JSON.stringify(TotalTime))

    res.send(response);
  } catch (error) {
    console.log(error);
    res.send(error.message);
  }
});

// Obtener solo un id en espcifico
app.get("/id/:id", async (req, res, next) => {
  try {

    // Verificar si la respuesta ya esta en Redis
    const hrstart = process.hrtime(); // time start

    const reply = await client_redis.get(req.params.id);
    if (reply) {
      const hrend = process.hrtime(hrstart); // time end
      console.log(`Tiempo transcurrido: ${hrend[0]}s ${hrend[1] / 1000000}ms`); // delta time
      console.log("Data obtenida desde cache");
      return res.send(JSON.parse(reply));
    }

    // Obtener info desde base de datos
    const response = await client_postgres.query('SELECT * FROM urgencias WHERE id = $1', [req.params.id]);

    const hrend = process.hrtime(hrstart); // time end
    console.log(`Tiempo transcurrido: ${hrend[0]}s ${hrend[1] / 1000000}ms`); // delta time
    
    // Guardar resultado en Redis con .set.
    const saveInRedis = await client_redis.set(req.params.id, JSON.stringify(response.rows)); //respuesta
    console.log("nueva data almacenada en cache:", saveInRedis);

    // reenviar al cliente

    console.log(response.length);
    res.send(response);
  }
  catch (error) {
    console.log(error);
    res.send(error.message);
  }
});

async function main() {// reenviar al cliente
  await client_redis.connect(); // conectarse con Redis
  await client_postgres.connect(); // conectarse a Postgres
  app.listen(3000);
  console.log("server listen on port 3000");
}

main();
