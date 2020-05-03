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
    const defaultSolrCore = config.get("solr.core");
    const wsName = config.get("digit.wsName");

    return {
        getDocsCountByEntityId: async function(entityId, coreName) {
            let solrCore;
            if (!coreName) {
                solrCore = defaultSolrCore;
            } else {
                solrCore = wsName + "_" + coreName;
            }
            const {data: {response:{numFound}}} = await axios.get(solrUrl + solrCore + "/select?q=entityId_sm:" + entityId + "&rows=0&start=0", {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8"
                }
            });

            return numFound;
        },
        getDocsCountByWorkflowId: async function(workflowId) {
            let solrCore = defaultSolrCore;
            const {data: {response:{numFound}}} = await axios.get(solrUrl + solrCore + "/select?q=workflowId_s:" + workflowId + "&rows=0&start=0", {
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
        getDBNamesMap: async function() {
            const DBNamesMap = new Map();

            const userCookie = await CookieManager.getActualCookie();
            const searchString = `select objectId, properties.dbName as dbName from (traverse * from (select entities from journalspec where name="UmlJournal")) where @class="EntitySpec" and properties.dbName is not null and properties.dbName <> "" limit -1`;

            let {"data":{"result":searchResult}} = await axios.post(orientUrl + `command/${orientDBName}/sql/-/20?format=rid,type,version,class,graph`, searchString, {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Cookie": userCookie
                }
            });
            for (let entityData of searchResult) {
                DBNamesMap.set(entityData.objectId, entityData.dbName);
            }

            return DBNamesMap;
        },
        getCustomSolrCoreEntitiesMap: async function() {
            const customSolrCoreEntitiesMap = new Map();

            const userCookie = await CookieManager.getActualCookie();
            const searchString = `select objectId, properties.solrCore as solrCore from (traverse * from (select entities from journalspec where name="UmlJournal")) where @class="EntitySpec" and properties.solrCore is not null and properties.solrCore <> "" limit -1`;

            let {"data":{"result":searchResult}} = await axios.post(orientUrl + `command/${orientDBName}/sql/-/20?format=rid,type,version,class,graph`, searchString, {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Cookie": userCookie
                }
            });
            for (let entityData of searchResult) {
                customSolrCoreEntitiesMap.set(entityData.objectId, entityData.solrCore);
            }

            return customSolrCoreEntitiesMap;
        },
        getDocsCountByClassName: async function(entityClassName) {
            const userCookie = await CookieManager.getActualCookie();
            const searchString = "SELECT count(*) FROM " + entityClassName + " WHERE (deleted = false or deleted is null)";

            let {"data":{"result":searchResult}} = await axios.post(orientUrl + `command/${orientDBName}/sql/-/20?format=rid,type,version,class,graph`, searchString, {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Cookie": userCookie
                }
            });
            searchResult = searchResult[0];
            return searchResult.count;
        },
        getDocsCountByWorkflowId: async function(workflowId) {
            const userCookie = await CookieManager.getActualCookie();
            const searchString = "SELECT count(*) FROM ProcessScope where workflow.objectId = \"" + workflowId + "\" AND processInstanceState <> \"TERMINATED\"";

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
app.use(cors()); //for using cors

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

let resultObject = {}

app.get("/checkAll", async (req, res) => {
    async function processFormElement(element, entitiesMap, linksMap){
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
                await processFormElement(fieldGroupElements[key], entitiesMap, linksMap);
            }
        }
    }
    async function processVisElement(visElement, entitiesMap, workflowsMap){
        let elementDataBind = visElement.dataBind;
        if (elementDataBind && visElement.properties) {
            let elementGridBindType = visElement.gridBindType;
            //default bindType is entity
            if (!elementGridBindType || elementGridBindType === "entity") {
                let elementModeReading = visElement.properties.modeReading;
                if (!elementModeReading || elementModeReading === "solr") {
                    await processEntityById(elementDataBind, entitiesMap);
                }
            } else if (elementGridBindType === "workflow") {
                let elementModeReading = visElement.properties.modeReading;
                if (!elementModeReading || elementModeReading === "solr") {
                    await processWorkflowById(elementDataBind, workflowsMap);
                }
            }
        }
        if (visElement.elements && visElement.elements.length > 0) {
            for (let element of visElement.elements) {
                await processVisElement(element, entitiesMap, workflowsMap);
            }
        }
    }
    async function processEntityById(entityId, entitiesMap){
        const entityData = entitiesMap.get(entityId);
        if (entityData && !entityData.checked) {
            let solrCoreName = entityData.solrCore;

            let [solrCount, orientCount] = await Promise.all([
                solrApp.getDocsCountByEntityId(entityId, solrCoreName),
                orientApp.getDocsCountByClassName(entityData.dbname)
            ]);
            entityData.solrCount = solrCount;
            entityData.orientCount = orientCount;
            
            if (solrCount !== orientCount) {
                entityData.hasDifference = true;
                entityData.delta = solrCount - orientCount;
            }

            entityData.checked = true;
        }
    }
    async function processWorkflowById(workflowId, workflowsMap){
        const workflowData = workflowsMap.get(workflowId);
        if (workflowData && !workflowData.checked) {
            let [solrCount, orientCount] = await Promise.all([
                solrApp.getDocsCountByWorkflowId(workflowId),
                orientApp.getDocsCountByWorkflowId(workflowId)
            ]);
            workflowData.solrCount = solrCount;
            workflowData.orientCount = orientCount;
            
            if (solrCount !== orientCount) {
                workflowData.hasDifference = true;
                workflowData.delta = solrCount - orientCount;
            }

            workflowData.checked = true;
        }
    }
    //типы элементов на форме
    const FORM_ELEMENT_TYPES = digitApp.FORM_ELEMENT_TYPES;
    try {
        res.send({
            code: 'OK'
        });

        logger.info("Let's scan " + digitAppUrl + " for missed solr documents");

        logger.info("Trying to get UMLSchema...");
        const {packages,entities} = await digitApp.getUMLSchema();
        logger.info("Total entities count is " + entities.length);
        
        logger.info("Trying to get dbnames map...");
        const DBNamesMap = await orientApp.getDBNamesMap();
        logger.info("Finished getting dbnames map");

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
                    let entityName = entity.properties.name, 
                        packageName = PackagesMap.get(entity.packageId),
                        dbName = DBNamesMap.get(entity.objectId);
                    //entity is not synchronized
                    if (!dbName) {
                        continue;
                    }

                    EntitiesMap.set(entity.objectId, {
                        "checked": false,
                        "dbname": dbName,
                        "umlName": packageName + "." + entityName,
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

        logger.info("Trying to get custom solr core entities map...");
        const CustomSolrCoreEntitiesMap = await orientApp.getCustomSolrCoreEntitiesMap();
        logger.info("Finished getting custom solr core entities map");

        for (let [entityId, solrCoreName] of CustomSolrCoreEntitiesMap) {
            let entityData = EntitiesMap.get(entityId);
            if (entityData) {
                entityData.solrCore = solrCoreName;
            }
        }
        logger.info("Finished custom solr cores processing");

        logger.info("Trying to get workflows...");
        const workflows = await digitApp.getWorkflows();
        logger.info("Total workflows count is " + workflows.length);
        
        const WorkflowsMap = new Map();
        for (let workflow of workflows) {
            let workflowObjectId = workflow.objectId;
            if (workflowObjectId) {
                if (!WorkflowsMap.has(workflowObjectId)) {
                    WorkflowsMap.set(workflowObjectId, {
                        "checked": false,
                        "name": workflow.title,
                        "hasDifference": false,
                        "delta": 0
                    });
                } else {
                    throw new Error("Workflow " + workflowObjectId + " has dublicate");
                }
            } else {
                throw new Error("There is workflow without objectId");
            }
        }
        logger.info("Finished workflows processing");

        logger.info("Trying to get forms...");
        let forms = await digitApp.getForms();
        logger.info("Total forms count is " + forms.length);
        
        const FORM_PROCESSING_LIMIT = config.get("other.formLimit");
        if (FORM_PROCESSING_LIMIT !== -1 && FORM_PROCESSING_LIMIT < forms.length) {
            forms.length = FORM_PROCESSING_LIMIT;
        }
        forms = forms.map(form => form.objectId);

        logger.info("Trying to get forms data...");
        const FORM_PROCESSING_FLOWS_COUNT = config.get("other.formFlowsCount");
        logger.info("Total flows count for forms is " + FORM_PROCESSING_FLOWS_COUNT);

        let formGettingFlows = [], formsIterator = forms[Symbol.iterator](),
            formDatas = [], formsDataReceivedAmount = 0;
        for (let i = 0; i < FORM_PROCESSING_FLOWS_COUNT; i++) {
            let newFlow = startFlow({
                execFunction: async function(formObjectId) {
                    let formData = await digitApp.getFormData(formObjectId);
                    formDatas.push(formData);
                    
                    formsDataReceivedAmount++;
                    if (formsDataReceivedAmount % 100 === 0) {
                        logger.info(formsDataReceivedAmount + " forms data received");
                    }
                },
                iterator: formsIterator
            });
            formGettingFlows.push(newFlow);
        }
        await Promise.all(formGettingFlows);
        logger.info("Finished getting forms data");

        logger.info("Starting forms processing...");
        let formsProcessedCount = 0;
        for (let formData of formDatas) {
            let elements = formData.elements;
            for (let element of elements) {
                await processFormElement(element, EntitiesMap, LinksMap);
            }
            formsProcessedCount++;
            if (formsProcessedCount % 100 === 0) {
                logger.info(formsProcessedCount + " forms processed");
            }
        }
        logger.info("Forms processing complete");

        let vises = await digitApp.getVises();
        logger.info("Total vises count is " + vises.length);

        const VISES_PROCESSING_LIMIT = config.get("other.visLimit");
        if (VISES_PROCESSING_LIMIT !== -1 && VISES_PROCESSING_LIMIT < vises.length) {
            vises.length = VISES_PROCESSING_LIMIT;
        }
        vises = vises.map(vis => vis.objectId);

        logger.info("Trying to get vises data...");
        const VIS_PROCESSING_FLOWS_COUNT = config.get("other.visFlowsCount");
        logger.info("Total flows count for vises is " + VIS_PROCESSING_FLOWS_COUNT);

        let visGettingFlows = [], visesIterator = vises[Symbol.iterator](),
            visDatas = [], visesDataReceivedAmount = 0;
        for (let i = 0; i < VIS_PROCESSING_FLOWS_COUNT; i++) {
            let newFlow = startFlow({
                execFunction: async function(visObjectId) {
                    let visData = await digitApp.getVisData(visObjectId);
                    visDatas.push(visData);
                    
                    visesDataReceivedAmount++;
                    if (visesDataReceivedAmount % 100 === 0) {
                        logger.info(visesDataReceivedAmount + " vises data received");
                    }
                },
                iterator: visesIterator
            });
            visGettingFlows.push(newFlow);
        }
        await Promise.all(visGettingFlows);
        logger.info("Finished getting vises data");

        logger.info("Starting vises processing...");
        let visesProcessedCount = 0;
        for (let visData of visDatas) {
            let elements = visData.elements;
            for (let element of elements) {
                await processVisElement(element, EntitiesMap, WorkflowsMap);
            }
            visesProcessedCount++;
            if (visesProcessedCount % 100 === 0) {
                logger.info(visesProcessedCount + " vises processed");
            }
        }
        logger.info("Vises processing complete");

        let resultEntitiesObject = {};
        for (let [entityId, entityData] of EntitiesMap) {
            if (entityData.hasDifference) {
                resultEntitiesObject[entityId] = {
                    "delta": entityData.delta,
                    "dbname": entityData.dbname,
                    "umlName": entityData.umlName,
                    "orientCount": entityData.orientCount,
                    "solrCount": entityData.solrCount
                }
            }
        }
        logger.info(resultEntitiesObject);
        
        let resultWorkflowObject = {};
        for (let [workflowId, workflowData] of WorkflowsMap) {
            if (workflowData.hasDifference) {
                resultWorkflowObject[workflowId] = {
                    "name": workflowData.name,
                    "delta": workflowData.delta,
                    "orientCount": workflowData.orientCount,
                    "solrCount": workflowData.solrCount
                }
            }
        }
        logger.info(resultWorkflowObject);

        resultObject = {
            resultEntitiesObject,
            resultWorkflowObject
        }

        logger.info("Operation completed");
    } catch (err) {
        logger.error(err);
    }
});

app.get("/getLastResult", async (req, res) => {
    try {
        res.send(resultObject);
    } catch (err) {
        res.sendStatus(400);

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

async function startFlow({execFunction,iterator}){
    let isRunning = true, nextIteration;
    while (isRunning) {
        nextIteration = iterator.next();
        if (!nextIteration.done) {
            await execFunction(nextIteration.value);
        } else {
            isRunning = false;
        }
    }
}