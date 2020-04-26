async function getFormsList(){

}

async function getVisesList(){

}

async function getUMLSchema(){

}

function getEntitiesListFromForm(){

}

function getEntitiesListFromVis(){

}

//import log4j subsystem
const logger = require("./core/logger.js");

const express = require('express');
const cors = require('cors');
const app = express();

const axios = require('axios');
const config = require("config");

app.use(express.json()) // for parsing application/json
app.use(express.urlencoded({ extended: true })) // for parsing application/x-www-form-urlencoded
//app.use(cors()); //for using cors

const port = config.get("application.port");
app.listen(port);

logger.info(`WebModule enabled on port: ${port}`);

app.get("/", async (req, res) => {
    res.sendStatus(200);
});

app.get("/start", async (req, res) => {



	res.send({
        code: 'OK'
    });
});