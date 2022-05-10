const axios = require('axios');

function SolrApp({solrUrl, defaultSolrCore, wsName}){
    async function makeSolrQuery({queryString, coreName, fieldList, limit, skip, additionalRequestPart}){
        if (!queryString) {
        	throw new Error("queryString is not defined");
        } else if (fieldList && !Array.isArray(fieldList)) {
        	throw new Error("fieldList must be an array");
        } else if (!limit && limit !== 0) {
        	throw new Error("limit is not defined");
        } else if (!skip && skip !== 0) {
        	throw new Error("skip is not defined");
        }

        let solrCore;
        if (coreName && coreName !== defaultSolrCore) {
            solrCore = wsName + "_" + coreName;
        } else {
            solrCore = defaultSolrCore;
        }

        let requestURL = `${solrCore}/select?q=${queryString}&rows=${limit}&start=${skip}`;
        if (fieldList && fieldList.length > 0) {
        	requestURL += `&fl=${fieldList.join(",")}`
        }
        if (additionalRequestPart) {
            requestURL += additionalRequestPart;
        }

        const {data} = await solrInstance.get(requestURL, {
            headers: {
                "Content-Type": "application/json;charset=UTF-8"
            }
        });

        return data;
    }

    async function getCoresSet() {
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

    const solrInstance = axios.create({
        "baseURL": solrUrl,
        "timeout": 60000
    });

    return {
        getDocsCountByEntityId: async function(entityId, coreName) {
            if (!entityId) {
            	throw new Error("entityId is not defined");
            }

            const queryString = "entityId_sm:" + entityId;
            const {response:{numFound}} = await makeSolrQuery({
            	"queryString": queryString, 
            	"coreName": coreName,
            	"limit": 0,
            	"skip": 0
            });

            return numFound;
        },
        getDocsCountByWorkflowId: async function(workflowId) {
            if (!workflowId) {
            	throw new Error("workflowId is not defined");
            } 

            let totalDocsCount = 0;

            const queryString = "workflowId_s:" + workflowId;
            const {response:{numFound}} = await makeSolrQuery({
            	"queryString": queryString,
            	"limit": 0,
            	"skip": 0
            });
            totalDocsCount += numFound;

            return totalDocsCount;
        },
        getTotalDocsCountByEntities: async function() {
            let totalDocsCount = 0;

            const queryString = "entityId_sm:[* TO *] && -(workflowId_s:[* TO *])";

            const allSolrCores = await getCoresSet();
            for (let solrCore of allSolrCores) {
                let {response:{numFound}} = await makeSolrQuery({
                	"queryString": queryString, 
                	"coreName": solrCore,
            		"limit": 0,
            		"skip": 0
                });
                totalDocsCount += numFound; 
            }

            return totalDocsCount;
        },
        getTotalDocsCountByWorkflows: async function() {
            const queryString = "-(entityId_sm:[* TO *]) && workflowId_s:[* TO *]";
            const {response:{numFound}} = await makeSolrQuery({
            	"queryString": queryString,
            	"limit": 0,
            	"skip": 0
            });

            return numFound;
        },
        getDocsVersions: async function({entityId, coreName, limit, skip}) {
        	if (!entityId) {
        		throw new Error("entityId is not defined");
        	} else if (!limit) {
        		throw new Error("limit is not defined");
        	} else if (!skip && skip !== 0) {
        		throw new Error("skip is not defined");
        	}

        	const queryString = `entityId_sm:${entityId}`;
            const {response:{docs}} = await makeSolrQuery({
            	"queryString": queryString,
            	"coreName": coreName, //can be undefined
            	"fieldList": ["id", "__orientVersion_d"],
            	"limit": limit,
            	"skip": skip
            });

            const docsVersionsData = [];
            for (let documentData of docs) {
            	let objectId = documentData.id,
            		version = documentData.__orientVersion_d;

            	if (objectId && version) {
            		docsVersionsData.push({
            			"objectId": objectId,
            			"version": version
            		});
            	}
            }

            return docsVersionsData;
        },
        getDocsVersionsSumByEntityId: async function(entityId, coreName) {
            if (!entityId) {
                throw new Error("entityId is not defined");
            }

            const queryString = "entityId_sm:" + entityId;
            const {stats:{stats_fields:{__orientVersion_d:{sum}}}} = await makeSolrQuery({
                "queryString": queryString,
                "coreName": coreName,
                "limit": 0,
                "skip": 0,
                "additionalRequestPart": "&stats=true&stats.field=__orientVersion_d"
            });

            return sum;
        }
    }
}

module.exports.SolrApp = SolrApp;