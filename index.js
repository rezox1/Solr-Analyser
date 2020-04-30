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

        logger.error(err);
    }
});

app.get("/checkAll", async (req, res) => {
    async function processElement(element, entitiesMap, linksMap){
        const elementType = element.objectType;
        if (elementType === FORM_ELEMENT_TYPES.LINK || elementType === FORM_ELEMENT_TYPES.TABLE) {
            await processFormLink(element, entitiesMap, linksMap);
        } else if (elementType === FORM_ELEMENT_TYPES.FIELD_GROUP) {
            await processFieldGroup(element, entitiesMap, linksMap);
        }
    }
    async function processFormLink(element, entitiesMap, linksMap){
        if (element.properties && element.properties.dataBind) {
            const entityId = linksMap.get(element.properties.dataBind);
            if (entityId) {
                await processEntityById(entityId, entitiesMap);
            }
        }
    }
    async function processFieldGroup(element, entitiesMap, linksMap){
        let fieldGroupElements = element.properties.elements;
        if (fieldGroupElements) {
            for (let key in fieldGroupElements) {
                await processElement(fieldGroupElements[key], entitiesMap, linksMap);
            }
        }
    }
    async function processEntityById(entityId, entitiesMap){
        const entityData = entitiesMap.get(entityId);
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
    //типы элементов на форме
    const FORM_ELEMENT_TYPES = digitApp.FORM_ELEMENT_TYPES;

    try {
        res.send({
            code: 'OK'
        });

        logger.info("Trying to get UMLSchema...");
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
        logger.info("Finished UML schema processing");

        const forms = await digitApp.getForms();
        logger.info("Total forms count is " + forms.length);
        
        let i = 0;
        for (let form of forms) {
            let {elements} = await digitApp.getFormData(form.objectId);
            for (let element of elements) {
                await processElement(element, EntitiesMap, LinksMap);
            }
            i++;
            if (i % 100 === 0) {
                logger.info(i + " forms processed");
            }
            if (i === 500) {
                break;
            }
        }

        /*
        const vises = await digitApp.getVises();
        logger.info("Total vises count is " + vises.length);
        */

        const resultEntitiesObject = {}

        for (let [entityId, entityData] of EntitiesMap) {
            if (entityData.hasDifference) {
                resultEntitiesObject[entityId] = {
                    "delta": entityData.delta,
                    "dbname": entityData.dbname
                }
            }
        }

        logger.info(resultEntitiesObject);
        logger.info("Operation completed");
    } catch (err) {
        logger.error(err);
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