//import log4j subsystem
const logger = require("./core/logger.js");

const express = require('express');
const cors = require('cors');
const app = express();

const axios = require('axios');
const config = require("config");
const {DigitApp} = require("digitjs");
const btoa = require('btoa');

const digitAppUrl = config.get("digit.url"),
    digitUsername = config.get("digit.username"),
    digitPassword = config.get("digit.password");

const digitApp = new DigitApp({
    "appUrl": digitAppUrl,
    "username": digitUsername,
    "password": digitPassword
});

const solrApp = (() => {
    const solrUrl = config.get("solr.url");
    const solrCore = config.get("solr.core");

    return {
        getDocsCount: async function(entityId){
            const {data: {response:{numFound}}} = await axios.get(solrUrl + solrCore + "/select?q=entityId_sm:" + entityId + "&rows=0&start=0", {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8"
                }
            });

            return numFound;
        }
    }
})();

const orientApp = (() => {
    const orientUrl = config.get("orientdb.url"),
        orientDBName = config.get("orientdb.dbname"),
        orientUsername = config.get("orientdb.username"),
        orientPassword = config.get("orientdb.password");

    const CookieManager = new globalCookieManager({
        "loginFunction": async function(){
            const loginData = await axios.get(orientUrl + "connect/" + orientDBName, {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Authorization": "Basic " + btoa(orientUsername + ":" + orientPassword)
                }
            });

            let RawUserCookie = loginData.headers["set-cookie"][0],
                UserCookie = RawUserCookie.substring(0, RawUserCookie.indexOf(";"));

            return UserCookie;
        }, 
        "checkCookieFunction": async function(){
            let checkCookieResult = true;
            
            try {
                const cookie = CookieManager.getCookie();
                const searchString = "SELECT count(*) FROM OUser";
                await axios.post(orientUrl + `command/${orientDBName}/sql/-/20?format=rid,type,version,class,graph`, searchString, {
                    headers: {
                        "Content-Type": "application/json;charset=UTF-8",
                        "Cookie": cookie
                    }
                });
            } catch (err) {
                checkCookieResult = false;
            }

            return checkCookieResult;
        }
    });

    return {
        getDocsCount: async function(entityClassName){
            const searchString = "SELECT count(*) FROM " + entityClassName + " WHERE (deleted = false or deleted is null)";
            const userCookie = await CookieManager.getActualCookie();

            let {"data":{"result":searchResult}} = await axios.post(orientUrl + `command/${orientDBName}/sql/-/20?format=rid,type,version,class,graph`, searchString, {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Cookie": userCookie
                }
            });
            searchResult = searchResult[0];
            return searchResult.count;
        }
    }
})();

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

app.get("/checkAll", async (req, res) => {
    function processFormLink(){
        
    }
    function fieldGroup(){

    }
    async function processEntityById(entityId, EntitiesMap){
        const entityData = EntitiesMap.get(entityId);
        if (!entityData.checked) {
            let [solrCount, orientCount] = await Promise.all([
                solrApp.getDocsCount(entityId),
                orientApp.getDocsCount(entityData.dbname)
            ]);
            
            if (solrCount !== orientCount) {
                entityData.hasDifference = true;
                entityData.delta = solrCount - orientCount;
            }

            entityData.checked = true;
        }
    }

    try {
        const {packages,entities} = await digitApp.getUMLSchema();
        logger.info("Total entities count is " + entities.length);
        
        const PackagesMap = new Map();
        for (let package of packages) {
            PackagesMap.set(package.objectId, package.properties.name);
        }

        const EntitiesMap = new Map(),
            LinksMap = new Map();        
        for (let entity of entities) {
            for (let link of entity.links) {
                if (link.objectId) {
                    if (!LinksMap.has(link.objectId)) {
                        LinksMap.set(link.objectId, link.properties.target);
                    } else {
                        logger.warn("Link " + link.objectId + " has dublicate");
                    }
                } else {
                    throw new Error("Entity " + entity.objectId + " has link without objectId");
                }
            }
            if (entity.objectId) {
                if (!EntitiesMap.has(entity.objectId)) {
                    EntitiesMap.set(entity.objectId, {
                        "checked": false,
                        "dbname": "DataEntity_" + PackagesMap.get(entity.packageId) + "_" + entity.properties.name,
                        "hasDifference": false,
                        "delta": 0
                    });
                } else {
                    logger.warn("Entity " + entity.objectId + " has dublicate");
                }
            } else {
                throw new Error("There is entity without objectId");
            }
        }

        /*
        const forms = await digitApp.getForms();
        logger.info("Total forms count is " + forms.length);

        const vises = await digitApp.getVises();
        logger.info("Total vises count is " + vises.length);
        */
        /*
        

        */

        await processEntityById("0a892aff-068f-312c-d780-3535182b2421", EntitiesMap);
        await processEntityById("8ebed96a-eef6-01d4-bff5-af57c67cc9df", EntitiesMap);
        await processEntityById("e666d3ed-3ce3-fab3-33b3-b3fc3b3dd3a3", EntitiesMap);
        await processEntityById("a996243d-6ede-615c-1c08-5639aa347210", EntitiesMap);
        await processEntityById("fa248ac2-f02d-a782-c7ef-70c716839d51", EntitiesMap);
        await processEntityById("3d3b767d-288e-619d-ea9e-1ebe6a30c915", EntitiesMap);
        


        const resultEntitiesObject = {}

        for (let [entityId, entityData] of EntitiesMap) {
            if (entityData.hasDifference) {
                resultEntitiesObject[entityId] = {
                    "delta": entityData.delta,
                    "dbname": entityData.dbname
                }
            }
        }

        res.send(resultEntitiesObject);
        
        /*
        res.send({
            code: 'OK'
        });
        */

        logger.info("Operation completed");
    } catch (err) {
        res.sendStatus(400);

        console.error(err);
    }
});

function globalCookieManager({loginFunction, checkCookieFunction}){
    async function refreshCookie() {
        cookie = await loginFunction();
        return cookie;
    }

    let cookie = null;

    this.getCookie = function() {
        return cookie;
    }
    this.getActualCookie = async function() {
        if (!cookie) {
            return await refreshCookie();
        } else {
            let checkCookieResult = await checkCookieFunction(cookie);
            if (checkCookieResult === true) {
                return cookie;
            } else {
                return await refreshCookie();
            }
        }
    }
    this.refreshCookie = refreshCookie;
}