//import log4j subsystem
const logger = require("./core/logger.js");

console.log = console.info = logger.info.bind(logger);
console.warn = logger.warn.bind(logger);
console.error = logger.error.bind(logger);
console.debug = logger.debug.bind(logger);

const express = require('express');
const cors = require('cors');
const app = express();

const NODE_ENV = process.env.NODE_ENV || "";
logger.info(`NODE_ENV = ${NODE_ENV}`);

const config = require("config");
const async = require("async");

const portForwardingEnable = config.get("portForwarding.enable");
if (portForwardingEnable === true) {
    logger.info("PortForwarding is enabled");

    const connecttionSetting = config.get("portForwarding.connectionSetting");

    const {SSHConnection} = require('node-ssh-forward');
    const sshConnection = new SSHConnection({
        endHost: connecttionSetting.host,
        endPort: connecttionSetting.port,
        username: connecttionSetting.username,
        password: connecttionSetting.password
    });

    const forwardSettings = config.get("portForwarding.forwardSettings");
    for (let forwardSetting of forwardSettings) {
        sshConnection.forward({
            fromPort: forwardSetting.remotePort,
            toPort: forwardSetting.remotePort,
            toHost: forwardSetting.remoteHost
        }).then(() => {
            logger.info(`Port ${forwardSetting.remotePort} for ${forwardSetting.remoteHost} successfully forwarded`);
        }).catch(errorData => {
            logger.error(errorData);

            logger.warning(`An error catched while forwarding ${forwardSetting.remotePort} port`);
        });
    }
}

const {DigitApp} = require("digitjs");
const {SolrApp} = require("./core/solr.js");
const {OrientDBApp} = require("./core/orientdb.js");

const dayjs = require("dayjs");
const customParseFormat = require('dayjs/plugin/customParseFormat');
dayjs.extend(customParseFormat);

const digitAppUrl = config.get("digit.url"),
    digitUsername = config.get("digit.username"),
    digitPassword = config.get("digit.password");

const digitApp = new DigitApp({
    "appUrl": digitAppUrl,
    "username": digitUsername,
    "password": digitPassword
});

const solrUrl = config.get("solr.url"),
	defaultSolrCore = config.get("solr.core"),
	wsName = config.get("digit.wsName");

const solrApp = new SolrApp({
	"solrUrl": solrUrl,
	"defaultSolrCore": defaultSolrCore,
	"wsName": wsName
});

const orientUrl = config.get("orientdb.url"),
	orientDBName = config.get("orientdb.dbname"),
	orientUsername = config.get("orientdb.username"),
	orientPassword = config.get("orientdb.password");

const orientApp = new OrientDBApp({
	"orientUrl": orientUrl,
	"orientDBName": orientDBName,
	"orientUsername": orientUsername,
	"orientPassword": orientPassword
});

app.use(express.json()) // for parsing application/json
app.use(express.urlencoded({ extended: true })) // for parsing application/x-www-form-urlencoded
app.use(cors()); //for using cors

const port = config.get("application.port");
app.listen(port);
logger.info(`WebModule enabled on port: ${port}`);

const CONNECTION_ERROR_CODES = ["ECONNABORTED", "ECONNRESET", "ETIMEDOUT"];

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

//map object for storing common workflows information (like total number of objects etc.)
const CommonWorkflowsInfoMap = new Map();
//map object for storing common entities information (like total number of objects etc.)
const CommonEntitiesInfoMap = new Map();

const FullCheckEntitiesMap = new Map();

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

        logger.info("Trying to get total count of objects...");
        
        const [totalWorkflowDocsCount, totalEntityDocsCount] = await Promise.all([
        	solrApp.getTotalDocsCountByWorkflows(),
        	solrApp.getTotalDocsCountByEntities()
        ]);
        CommonWorkflowsInfoMap.set("totalDocsCount", totalWorkflowDocsCount);
        CommonEntitiesInfoMap.set("totalDocsCount", totalEntityDocsCount);

        logger.info("Finished getting total count of objects");

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

        let formDatas = [], formsDataReceivedAmount = 0;
        await async.eachLimit(forms, FORM_PROCESSING_FLOWS_COUNT, async function getFormDataById(formObjectId) {
            try {
                let formData = await digitApp.getFormData(formObjectId);
                formDatas.push(formData);
            
                formsDataReceivedAmount++;
                if (formsDataReceivedAmount % 100 === 0) {
                    logger.info(formsDataReceivedAmount + " forms data received");
                }
            } catch (err) {
                if (CONNECTION_ERROR_CODES.includes(err.code)) {
                    logger.warn("There are connection troubles...");

                    return await getFormDataById.apply(this, arguments);
                } else {
                    throw err;
                }
            }
        });
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

        let visDatas = [], visesDataReceivedAmount = 0;
        await async.eachLimit(vises, VIS_PROCESSING_FLOWS_COUNT, async function getVisDataById(visObjectId) {
            try {
                let visData = await digitApp.getVisData(visObjectId);
                visDatas.push(visData);
                
                visesDataReceivedAmount++;
                if (visesDataReceivedAmount % 100 === 0) {
                    logger.info(visesDataReceivedAmount + " vises data received");
                }
            } catch (err) {
                if (CONNECTION_ERROR_CODES.includes(err.code)) {
                    logger.warn("There are connection troubles...");

                    return await getVisDataById.apply(this, arguments);
                } else {
                    throw err;
                }
            }
        });
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
        let totalDocsCount = CommonEntitiesInfoMap.get("totalDocsCount");
        if (totalDocsCount) {
        	resultObject.totalDocsCount = totalDocsCount;
        } else {
        	resultObject.totalDocsCount = 0;
        }

        let customSolrCore;
        const entitiesData = {}
        for (let [entityId, entityData] of EntitiesMap) {
            if (entityData.checked) {
                entitiesData[entityId] = {
                    "umlName": entityData.umlName,
                    "solrCount": entityData.solrCount
                }
                customSolrCore = entityData.solrCore;
                if (customSolrCore) {
                	entitiesData[entityId].customSolrCore = wsName + "_" + customSolrCore;
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
        let totalDocsCount = CommonWorkflowsInfoMap.get("totalDocsCount");
        if (totalDocsCount) {
        	resultObject.totalDocsCount = totalDocsCount;
        } else {
        	resultObject.totalDocsCount = 0;
        }

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
                	"umlName": entityData.umlName + " - " + entityData.orientCount.toFixed() + "/" + entityData.solrCount.toFixed()
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

app.get("/getWorkflowsList", async (req, res) => {
    try {
        const workflowsList = [];

        for (let [workflowId, workflowData] of WorkflowsMap) {
            if (workflowData.checked) {
                let item = {
                    "workflowId": workflowId,
                    "name": workflowData.name + " - " + workflowData.orientCount.toFixed() + "/" + workflowData.solrCount.toFixed()
                }
                workflowsList.push(item);
            }
        }

        res.send(workflowsList);
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

const SET_TIMEOUT_LIMIT = 2147483647;

app.get("/syncEntity/:entityId", async (req, res) => {
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

app.get("/syncAllWrongDocuments/:entityId", async (req, res) => {
    try {
        const entityId = req.params.entityId;
        if (!entityId) {
            throw new Error("entityId is not defined");
        }

        res.sendStatus(200);

        try {
            await syncAllWrongEntityDocuments(entityId);
        } catch (err) {
            logger.error(err);
        }
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

app.get("/startFullCheck/:entityId", async (req, res) => {
    try {
        const entityId = req.params.entityId;
        if (!entityId) {
            throw new Error("entityId is not defined");
        }
        const entityData = EntitiesMap.get(entityId);
        if (!entityData) {
            throw new Error("Unknown entityId: " + entityId);
        }

        res.sendStatus(200);

        try {
            await doEntityFullCheck(entityId, entityData);
        } catch (err) {
            logger.error(err);
        }
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

app.get("/getFullCheckData/:entityId", async (req, res) => {
    try {
    	const entityId = req.params.entityId;
        if (!entityId) {
        	throw new Error("entityId is not defined");
        }
        const entityData = EntitiesMap.get(entityId);
        if (!entityData) {
        	throw new Error("Unknown entityId: " + entityId);
        }

        const fullCheckData = FullCheckEntitiesMap.get(entityId);
        if (fullCheckData) {
        	res.send({
        		"notExistDocs": [...fullCheckData.notExistDocs],
        		"existDocs": [...fullCheckData.existDocs],
        		"differentVersions": [...fullCheckData.differentVersions]
        	});
        } else {
        	res.send({});
        }
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

app.get("/syncAllEntityDocumentsWithDifference", async (req, res) => {
    try {
        logger.info("Start synchronization of all entity documents with difference");

        res.sendStatus(200);

        try {
            for (let [entityId, entityData] of EntitiesMap) {
                if (entityData.hasDifference) {
                    await syncAllWrongEntityDocuments(entityId);
                }
            }

            logger.info("Synchronization of all entity documents with difference is completed");
        } catch (err) {
            logger.error(err);
        }
    } catch (err) {
        res.sendStatus(400);

        logger.error(err);
    }
});

async function processEntityById(entityId, entitiesMap) {
    const entityData = entitiesMap.get(entityId);
    if (entityData && !entityData.checked) {
        let solrCoreName = entityData.solrCore;
        try {
            let [solrCount, orientCount] = await Promise.all([
                solrApp.getDocsCountByEntityId(entityId, solrCoreName),
                orientApp.getDocsCountByClassName(entityData.dbname)
            ]);

            entityData.solrCount = solrCount;
            entityData.orientCount = orientCount;
            entityData.hasDifference = false;
            entityData.delta = solrCount - orientCount;

            if (entityData.delta) {
                entityData.hasDifference = true;
            } else {
                let [solrDocsVersionsSum, orientDocsVersionsSum] = await Promise.all([
                    solrApp.getDocsVersionsSumByEntityId(entityId, solrCoreName),
                    orientApp.getDocsVersionsSumByClassName(entityData.dbname)
                ]);

                if (solrDocsVersionsSum !== orientDocsVersionsSum) {
                    entityData.hasDifference = true;
                }
            }

            entityData.checked = true;
        } catch (err) {
            if (CONNECTION_ERROR_CODES.includes(err.code)) {
                logger.warn("There are connection troubles...");

                await processEntityById.apply(this, arguments);
                return;
            } else {
                throw err;
            }
        }   
    }
}

async function processWorkflowById(workflowId, workflowsMap) {
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

async function doEntityFullCheck(entityId, entityData) {
    function* getProcessedCount(totalCount, processingLimit){
        let processedCount = 0;
        while (processedCount < totalCount) {
            yield processedCount;

            processedCount += processingLimit;
        }
    }

    if (!entityId) {
        throw new Error("entityId is not defined");
    } else if (!entityData) {
        throw new Error("entityData is not defined");
    }

    logger.info("Start full check of " + entityData.umlName);

    entityData.checked = false;
    //refresh data about solr and orient documents count
    await processEntityById(entityId, EntitiesMap);

    let {solrCount, orientCount} = entityData;

    const orientDocsMap = new Map();
    const solrDocsMap = new Map();

    const docsGettingFlows = [];

    const SOLR_GETTING_LIMIT = 10000,
        processedSolrGenerator = getProcessedCount(solrCount, SOLR_GETTING_LIMIT);

    const solrDocsGettingFlow = async.eachLimit(processedSolrGenerator, 1, async function getSolrDocsVersions(skipCount) {
        try {
            let docsVersionsData = await solrApp.getDocsVersions({
                "entityId": entityId,
                "coreName": entityData.solrCore,
                "limit": SOLR_GETTING_LIMIT,
                "skip": skipCount
            });
            for (let documentData of docsVersionsData) {
                solrDocsMap.set(documentData.objectId, documentData.version);
            }

            logger.info(`${solrDocsMap.size} of ${solrCount} solr documents received`);
        } catch (err) {
            if (CONNECTION_ERROR_CODES.includes(err.code)) {
                logger.warn("There are connection troubles...");

                return await getSolrDocsVersions.apply(this, arguments);
            } else {
                throw err;
            }
        }
    });
    docsGettingFlows.push(solrDocsGettingFlow);

    const ORIENT_GETTING_LIMIT = 10000,
        processedOrientGenerator = getProcessedCount(orientCount, ORIENT_GETTING_LIMIT);

    const orientDocsGettingFlow = async.eachLimit(processedOrientGenerator, 1, async function getOrientDocsVersions(skipCount) {
        try {
            let docsVersionsData = await orientApp.getDocsVersions({
                "entityClassName": entityData.dbname,
                "limit": ORIENT_GETTING_LIMIT,
                "skip": skipCount
            });
            for (let documentData of docsVersionsData) {
                orientDocsMap.set(documentData.objectId, documentData.version);
            }

            logger.info(`${orientDocsMap.size} of ${orientCount} orient documents received`);
        } catch (err) {
            if (CONNECTION_ERROR_CODES.includes(err.code)) {
                logger.warn("There are connection troubles...");

                return await getOrientDocsVersions.apply(this, arguments);
            } else {
                throw err;
            }
        }
    });
    docsGettingFlows.push(orientDocsGettingFlow);

    await Promise.all(docsGettingFlows);
    logger.info("Finished getting docs versions data");

    const notExistDocs = new Set(),
        existDocs = new Set(),
        differentVersions = new Set();
    const fullCheckEntityData = {
        "notExistDocs": notExistDocs,
        "existDocs": existDocs,
        "differentVersions": differentVersions
    }
    FullCheckEntitiesMap.set(entityId, fullCheckEntityData);

    let solrDocVersion = 0;
    for (let [objectId, orientDocVersion] of orientDocsMap) {
        solrDocVersion = solrDocsMap.get(objectId);
        if (solrDocVersion) {
            if (orientDocVersion !== solrDocVersion) {
                differentVersions.add(objectId);
            }

            solrDocsMap.delete(objectId);
        } else {
            notExistDocs.add(objectId);
        }

        orientDocsMap.delete(objectId);
    }
    for (let objectId of solrDocsMap.keys()) {
        existDocs.add(objectId);

        solrDocsMap.delete(objectId);
    }

    logger.info("Full check of entity '" + entityData.umlName + "' is completed");

    return fullCheckEntityData;
}

async function syncAllWrongEntityDocuments(entityId) {
    async function performObjectIdsToDelete() {
        if (objectIdsToDelete.length > 0) {
            logger.info('Send request to delete unnecessary documents from solr');

            await digitApp.deleteObjects(objectIdsToDelete);

            logger.info('Unnecessary documents was deleted from solr');

            //to repeat processing
            entityData.checked = false;
            await processEntityById(entityId, EntitiesMap);
        }
    }

    async function performObjectIdsToRecovery() {
        if (objectIdsToRecovery.length > 0) {
            let transactionId = await digitApp.syncDocuments({
                "entityId": entityId,
                "objectIds": objectIdsToRecovery
            });

            logger.info('Start documents synchronization of "' + entityName + '"');

            let transactionInProgress = true;
            while (transactionInProgress) {
                let transactionPercent = await getTransactionPercent(transactionId);
                if (transactionPercent >= 100) {
                    transactionInProgress = false;

                    let entityData = EntitiesMap.get(entityId);
                    //to repeat processing
                    entityData.checked = false;
                    await processEntityById(entityId, EntitiesMap);

                    logger.info('Synchronization documents of "' + entityName + '" is completed');
                } else {
                    if (transactionPercent > 0) {
                        logger.info('Current transaction percent for "' + entityName + '" documents is ' + transactionPercent);
                    }

                    await sleep(10000);
                }
            }
        }
    }

    if (!entityId) {
        throw new Error("entityId is not defined");
    }

    const entityData = EntitiesMap.get(entityId);
    const entityName = entityData.umlName;

    let fullCheckData = FullCheckEntitiesMap.get(entityId);
    if (!fullCheckData) {
        fullCheckData = await doEntityFullCheck(entityId, entityData);
    }

    let objectIdsToRecovery = [...fullCheckData.notExistDocs, ...fullCheckData.differentVersions];

    let objectIdsToDelete = [...fullCheckData.existDocs];

    await Promise.all([
        performObjectIdsToDelete(),
        performObjectIdsToRecovery()
    ]);

    await doEntityFullCheck(entityId, entityData);
}

async function getTransactionPercent(transactionId) {
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

function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}
