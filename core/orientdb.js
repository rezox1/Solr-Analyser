const axios = require('axios');
const btoa = require('btoa');

const CONNECTION_ERROR_CODES = ["ECONNABORTED", "ECONNRESET", "ETIMEDOUT"];

function OrientDBApp({orientUrl, orientDBName, orientUsername, orientPassword}){
    async function makeQuery(queryString){
        if (!queryString) {
        	throw new Error("queryString is not defined");
        }

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
                    console.warn("There are connection troubles...");

                    return await checkCookieFunction.apply(this, arguments);
                } else {
                    console.error("Error while evaluating checkCookieFunction from orient's inctance: " + err);

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
            if (!entityClassName) {
            	throw new Error("entityClassName is not defined");
            }

            const searchString = "SELECT count(*) FROM " + entityClassName + " WHERE (deleted = false or deleted is null)";

            const [searchResult] = await makeQuery(searchString);
            return searchResult.count;
        },
        getDocsCountByWorkflowId: async function(workflowId) {
            if (!workflowId) {
            	throw new Error("workflowId is not defined");
            }

            const searchString = "SELECT count(*) FROM ProcessScope where workflow.objectId = \"" + workflowId + "\" AND processInstanceState <> \"TERMINATED\"";

            const [searchResult] = await makeQuery(searchString);
            return searchResult.count;
        },
        getDocsVersions: async function({entityClassName, limit, skip}){
        	if (!entityClassName) {
        		throw new Error("entityClassName is not defined");
        	} else if (!limit) {
        		throw new Error("limit is not defined");
        	} else if (!skip && skip !== 0) {
        		throw new Error("skip is not defined");
        	}

        	const searchString = `SELECT objectId, @version FROM ${entityClassName} WHERE (deleted = false or deleted is null) LIMIT ${limit} SKIP ${skip}`;

        	const searchResult = await makeQuery(searchString);

        	const docsVersionsData = [];
        	for (let documentData of searchResult) {
        		docsVersionsData.push({
        			"objectId": documentData.objectId,
        			"version": documentData.version
        		});
        	}

        	return docsVersionsData;
        }
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

module.exports.OrientDBApp = OrientDBApp;