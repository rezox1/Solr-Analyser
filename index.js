//import log4j subsystem
const logger = require("./core/logger.js");

const express = require('express');
const cors = require('cors');
const app = express();

const axios = require('axios');
const config = require("config");
const {DigitApp} = require("digitjs");
const btoa = require('btoa');

const dayjs = require("dayjs");
const customParseFormat = require('dayjs/plugin/customParseFormat');
dayjs.extend(customParseFormat);

console.log = console.info = logger.info.bind(logger);
console.warn = logger.warn.bind(logger);
console.error = logger.error.bind(logger);

const digitAppUrl = config.get("digit.url"),
    digitUsername = config.get("digit.username"),
    digitPassword = config.get("digit.password");

const digitApp = new DigitApp({
    "appUrl": digitAppUrl,
    "username": digitUsername,
    "password": digitPassword
});

const solrApp = (() => {
    async function makeSolrQuery(queryString, coreName){
        let solrCore;
        if (coreName && coreName !== defaultSolrCore) {
            solrCore = wsName + "_" + coreName;
        } else {
            solrCore = defaultSolrCore;
        }

        const {data:{response}} = await solrInstance.get(solrCore + "/select?q=" + queryString + "&rows=0&start=0", {
            headers: {
                "Content-Type": "application/json;charset=UTF-8"
            }
        });

        return response;
    }
    async function getCoresSet(){
        const coresSet = new Set();
        coresSet.add(defaultSolrCore);

        const {"data":{"status":coresStatusInfo}} = await solrInstance.get("admin/cores?indexInfo=false", {
            headers: {
                "Content-Type": "application/json;charset=UTF-8"
            }
        });
        for (let coreName in coresStatusInfo) {
            if (coreName.startsWith(wsName) && coreName !== defaultSolrCore) {
                let internalCoreName = coreName.replace(wsName + "_", "");
                coresSet.add(internalCoreName);
            }
        }

        return coresSet;
    }

    const solrUrl = config.get("solr.url"),
        defaultSolrCore = config.get("solr.core");
    
    const wsName = config.get("digit.wsName");

    const solrInstance = axios.create({
        "baseURL": solrUrl,
        "timeout": 60000
    });

    return {
        getDocsCountByEntityId: async function(entityId, coreName) {
            const queryString = "entityId_sm:" + entityId;
            const {numFound} = await makeSolrQuery(queryString, coreName);

            return numFound;
        },
        getDocsCountByWorkflowId: async function(workflowId) {
            let totalDocsCount = 0;

            const queryString = "workflowId_s:" + workflowId;
            const {numFound} = await makeSolrQuery(queryString);
            totalDocsCount += numFound;

            return totalDocsCount;
        },
        getTotalDocsCountByEntities: async function() {
            let totalDocsCount = 0;

            const queryString = "entityId_sm:[* TO *] && -(workflowId_s:[* TO *])";

            const allSolrCores = await getCoresSet();
            for (let solrCore of allSolrCores) {
                let {numFound} = await makeSolrQuery(queryString, solrCore);
                totalDocsCount += numFound; 
            }

            return totalDocsCount;
        },
        getTotalDocsCountByWorkflows: async function() {
            const queryString = "-(entityId_sm:[* TO *]) && workflowId_s:[* TO *]";
            const {numFound} = await makeSolrQuery(queryString);

            return numFound;
        }
    }
})();

const orientApp = (() => {
    async function makeQuery(queryString){
        const userCookie = await CookieManager.getActualCookie();
        const {
            "data":{"result":searchResult}
        } = await orientInstance.post(`command/${orientDBName}/sql/-/20?format=rid,type,version,class,graph`, queryString, {
            headers: {
                "Content-Type": "application/json;charset=UTF-8",
                "Cookie": userCookie
            }
        });

        return searchResult;
    }

    const orientUrl = config.get("orientdb.url"),
        orientDBName = config.get("orientdb.dbname"),
        orientUsername = config.get("orientdb.username"),
        orientPassword = config.get("orientdb.password");

    const orientInstance = axios.create({
        "baseURL": orientUrl,
        "timeout": 60000
    });

    const CookieManager = new globalCookieManager({
        "loginFunction": async function loginFunction(){
            const loginData = await orientInstance.get("connect/" + orientDBName, {
                headers: {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Authorization": "Basic " + btoa(orientUsername + ":" + orientPassword)
                }
            });

            let RawUserCookie = loginData.headers["set-cookie"][0],
                UserCookie = RawUserCookie.substring(0, RawUserCookie.indexOf(";"));

            return UserCookie;
        }, 
        "checkCookieFunction": async function checkCookieFunction(){
            let checkCookieResult = true;
            
            try {
                const cookie = CookieManager.getCookie();
                const searchString = "SELECT count(*) FROM OUser";
                await orientInstance.post(`command/${orientDBName}/sql/-/20?format=rid,type,version,class,graph`, searchString, {
                    headers: {
                        "Content-Type": "application/json;charset=UTF-8",
                        "Cookie": cookie
                    }
                });
            } catch (err) {
                if (CONNECTION_ERROR_CODES.includes(err.code)) {
                    logger.warn("There are connection troubles...");

                    return await checkCookieFunction.apply(this, arguments);
                } else {
                    logger.error("Error while evaluating checkCookieFunction from orient's inctance: " + err);

                    checkCookieResult = false;
                }
            }
            return checkCookieResult;
        }
    });

    return {
        getDBNamesMap: async function() {
            const searchString = `select objectId, properties.dbName as dbName from (traverse * from (select entities from journalspec where name="UmlJournal")) where @class="EntitySpec" and properties.dbName is not null and properties.dbName <> "" limit -1`;

            const searchResult = await makeQuery(searchString);

            const DBNamesMap = new Map();

            for (let entityData of searchResult) {
                DBNamesMap.set(entityData.objectId, entityData.dbName);
            }

            return DBNamesMap;
        },
        getCustomSolrCoreEntitiesMap: async function() {
            const searchString = `select objectId, properties.solrCore as solrCore from (traverse * from (select entities from journalspec where name="UmlJournal")) where @class="EntitySpec" and properties.solrCore is not null and properties.solrCore <> "" limit -1`;
            
            const searchResult = await makeQuery(searchString);

            const customSolrCoreEntitiesMap = new Map();

            for (let entityData of searchResult) {
                customSolrCoreEntitiesMap.set(entityData.objectId, entityData.solrCore);
            }

            return customSolrCoreEntitiesMap;
        },
        getDocsCountByClassName: async function(entityClassName) {
            const searchString = "SELECT count(*) FROM " + entityClassName + " WHERE (deleted = false or deleted is null)";

            const [searchResult] = await makeQuery(searchString);
            return searchResult.count;
        },
        getDocsCountByWorkflowId: async function(workflowId) {
            const searchString = "SELECT count(*) FROM ProcessScope where workflow.objectId = \"" + workflowId + "\" AND processInstanceState <> \"TERMINATED\"";

            const [searchResult] = await makeQuery(searchString);
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

const CONNECTION_ERROR_CODES = ["ECONNABORTED", "ECONNRESET"];

app.get("/", async (req, res) => {
    try {
        res.sendStatus(200);
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

//map object for storing workflows information
const WorkflowsMap = new Map();
//map object for storing entities information
const EntitiesMap = new Map();

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

        EntitiesMap.clear();

        const LinksMap = new Map();        
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
        
        WorkflowsMap.clear();
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
                execFunction: async function getFormDataById(formObjectId) {
                    let formData;
                    try {
                        formData = await digitApp.getFormData(formObjectId);
                    } catch (err) {
                        if (CONNECTION_ERROR_CODES.includes(err.code)) {
                            logger.warn("There are connection troubles...");

                            await getFormDataById.apply(this, arguments);
                            return;
                        } else {
                            throw err;
                        }
                    }
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
                execFunction: async function getVisDataById(visObjectId) {
                    let visData;
                    try {
                        visData = await digitApp.getVisData(visObjectId);
                    } catch (err) {
                        if (CONNECTION_ERROR_CODES.includes(err.code)) {
                            logger.warn("There are connection troubles...");

                            await getVisDataById.apply(this, arguments);
                            return;
                        } else {
                            throw err;
                        }
                    }
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

        logger.info("Operation completed");
    } catch (err) {
        logger.error(err);
    }
});

app.get("/getLastResult", async (req, res) => {
    try {
        let resultObject = {}

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

        resultObject = {
            resultEntitiesObject,
            resultWorkflowObject
        }

        res.send(resultObject);
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

app.get("/getSolrEntitiesMap", async (req, res) => {
    try {
        const resultObject = {}
        resultObject.totalDocsCount = await solrApp.getTotalDocsCountByEntities();

        const entitiesData = {}
        for (let [entityId, entityData] of EntitiesMap) {
            if (entityData.checked) {
                entitiesData[entityId] = {
                    "umlName": entityData.umlName,
                    "solrCount": entityData.solrCount
                }
            }
        }
        resultObject.entitiesData = entitiesData;

        res.send(resultObject);
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

app.get("/getSolrWorkflowsMap", async (req, res) => {
    try {
        const resultObject = {}
        resultObject.totalDocsCount = await solrApp.getTotalDocsCountByWorkflows();

        const workflowsData = {}
        for (let [workflowId, workflowData] of WorkflowsMap) {
            if (workflowData.checked) {
                workflowsData[workflowId] = {
                    "name": workflowData.name,
                    "solrCount": workflowData.solrCount
                }
            }
        }
        resultObject.workflowsData = workflowsData;

        res.send(resultObject);
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

app.get("/getEntitiesList", async (req, res) => {
    try {
        const entitiesList = [];

        for (let [entityId, entityData] of EntitiesMap) {
            if (entityData.checked) {
                let item = {
                	"entityId": entityId,
                	"umlName": entityData.umlName
                }
                entitiesList.push(item);
            }
        }

        res.send(entitiesList);
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

const SET_TIMEOUT_LIMIT = 2147483647;

app.get("/syncEntity/:entityId", async (req, res) => {
    async function getTransactionPercent(transactionId){
		try {
			let {"percent": transactionPercent} = await digitApp.getTransactionData(transactionId);
			return transactionPercent;
		} catch (err) {
			if (CONNECTION_ERROR_CODES.includes(err.code)) {
				logger.warn("There are connection troubles...");

				transactionPercent = await getTransactionPercent.apply(this, arguments);
				return transactionPercent;
			} else {
				throw err;
			}
		}
	}

    try {
        const entityId = req.params.entityId;
        if (!entityId) {
        	throw new Error("entityId is not defined");
        }
        const {"umlName": entityName} = EntitiesMap.get(entityId);

        let startAt = req.query.startAt;
        if (startAt) {
        	let startAtDate = dayjs(startAt, "DD.MM.YYYY HH:mm");
        	if (startAtDate.isValid()) {
        		let currentDate = dayjs();
        		if (currentDate.isBefore(startAtDate)) {
        			let datesDifference = startAtDate.diff(currentDate);
        			if (datesDifference > SET_TIMEOUT_LIMIT) {
        				throw new Error("Synchronization date is too big");
        			} else {
        				logger.info('Schedule synchronization of entity "' + entityName + '" at ' + startAt);

        				res.sendStatus(200);

        				await sleep(datesDifference);
        			}
        		} else {
        			logger.info('Synchronization of "' + entityName + '" will be started immediately');
        		}
        	} else {
        		throw new Error("Incorrect date format");
        	}
        }

        const transactionId = await digitApp.syncEntity(entityId);
		
		logger.info('Start synchronization of "' + entityName + '"');
		if (!res.headersSent) {
			res.sendStatus(200);
		}

		try {
			let transactionInProgress = true;
			while (transactionInProgress) {
				let transactionPercent = await getTransactionPercent(transactionId);
				if (transactionPercent >= 100) {
					transactionInProgress = false;

					let entityData = EntitiesMap.get(entityId);
					//to repeat processing
					entityData.checked = false;
					await processEntityById(entityId, EntitiesMap);

					logger.info('Synchronization of "' + entityName + '" is completed');
				} else {
					if (transactionPercent > 0) {
						logger.info('Current transaction percent for "' + entityName + '" is ' + transactionPercent);
					}

					await sleep(10000);
				}
			}
		} catch (err) {
			logger.error(err);
		}
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

async function processEntityById(entityId, entitiesMap){
    const entityData = entitiesMap.get(entityId);
    if (entityData && !entityData.checked) {
        let solrCoreName = entityData.solrCore;

        let solrCount = 0, orientCount = 0;
        try {
            [solrCount, orientCount] = await Promise.all([
                solrApp.getDocsCountByEntityId(entityId, solrCoreName),
                orientApp.getDocsCountByClassName(entityData.dbname)
            ]);
        } catch (err) {
            if (CONNECTION_ERROR_CODES.includes(err.code)) {
                logger.warn("There are connection troubles...");

                await processEntityById.apply(this, arguments);
                return;
            } else {
                throw err;
            }
        }
        entityData.solrCount = solrCount;
        entityData.orientCount = orientCount;
        entityData.hasDifference = false;
        entityData.delta = 0;
        
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
        let solrCount = 0, orientCount = 0;
        try {
            [solrCount, orientCount] = await Promise.all([
                solrApp.getDocsCountByWorkflowId(workflowId),
                orientApp.getDocsCountByWorkflowId(workflowId)
            ]);
        } catch (err) {
            if (CONNECTION_ERROR_CODES.includes(err.code)) {
                logger.warn("There are connection troubles...");

                await processWorkflowById.apply(this, arguments);
                return;
            } else {
                throw err;
            }
        }
        workflowData.solrCount = solrCount;
        workflowData.orientCount = orientCount;
        
        if (solrCount !== orientCount) {
            workflowData.hasDifference = true;
            workflowData.delta = solrCount - orientCount;
        }

        workflowData.checked = true;
    }
}

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

function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}