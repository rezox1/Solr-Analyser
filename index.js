//import log4j subsystem
const logger = require("./core/logger.js");

const express = require('express');
const cors = require('cors');
const app = express();

const axios = require('axios');
const config = require("config");
const {DigitApp} = require("digitjs");

const digitAppUrl = config.get("digit.url"),
    digitUsername = config.get("digit.username"),
    digitPassword = config.get("digit.password");

const digitApp = new DigitApp({
    "appUrl": digitAppUrl,
    "username": digitUsername,
    "password": digitPassword
});

app.use(express.json()) // for parsing application/json
app.use(express.urlencoded({ extended: true })) // for parsing application/x-www-form-urlencoded
//app.use(cors()); //for using cors

const port = config.get("application.port");
app.listen(port);

logger.info(`WebModule enabled on port: ${port}`);

app.get("/", async (req, res) => {
    try {
        res.sendStatus(200);
    } catch (err) {
        res.sendStatus(400);

        console.error(err);
    }
});

app.get("/start", async (req, res) => {
    try {
        const forms = await digitApp.getForms();
        const vises = await digitApp.getVises();
        res.send(vises[0]);
        
        /*
        res.send({
            code: 'OK'
        });
        */
    } catch (err) {
        res.sendStatus(400);

        console.error(err);
    }
});