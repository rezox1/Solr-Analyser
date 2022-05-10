const {OrientDBApp} = require("orient-http-client");

OrientDBApp.prototype.getDBNamesMap = async function() {
    const searchString = `select objectId, properties.dbName as dbName from (traverse * from (select entities from journalspec where name="UmlJournal")) where @class="EntitySpec" and properties.dbName is not null and properties.dbName <> "" limit -1`;

    const searchResult = await this.makeQuery(searchString);

    const DBNamesMap = new Map();

    for (let entityData of searchResult) {
        DBNamesMap.set(entityData.objectId, entityData.dbName);
    }

    return DBNamesMap;
}

OrientDBApp.prototype.getCustomSolrCoreEntitiesMap = async function() {
    const searchString = `select objectId, properties.solrCore as solrCore from (traverse * from (select entities from journalspec where name="UmlJournal")) where @class="EntitySpec" and properties.solrCore is not null and properties.solrCore <> "" limit -1`;
    
    const searchResult = await this.makeQuery(searchString);

    const customSolrCoreEntitiesMap = new Map();

    for (let entityData of searchResult) {
        customSolrCoreEntitiesMap.set(entityData.objectId, entityData.solrCore);
    }

    return customSolrCoreEntitiesMap;
}

OrientDBApp.prototype.getDocsCountByClassName = async function(entityClassName) {
    if (!entityClassName) {
        throw new Error("entityClassName is not defined");
    }

    const searchString = "SELECT count(*) FROM " + entityClassName + " WHERE (deleted = false or deleted is null)";

    const [searchResult] = await this.makeQuery(searchString);
    return searchResult.count;
}

OrientDBApp.prototype.getDocsCountByWorkflowId = async function(workflowId) {
    if (!workflowId) {
        throw new Error("workflowId is not defined");
    }

    const searchString = "SELECT count(*) FROM ProcessScope where workflow.objectId = \"" + workflowId + "\" AND processInstanceState <> \"TERMINATED\"";

    const [searchResult] = await this.makeQuery(searchString);
    return searchResult.count;
}

OrientDBApp.prototype.getDocsVersions = async function({entityClassName, limit, skip}){
    if (!entityClassName) {
        throw new Error("entityClassName is not defined");
    } else if (!limit) {
        throw new Error("limit is not defined");
    } else if (!skip && skip !== 0) {
        throw new Error("skip is not defined");
    }

    const searchString = `SELECT objectId, @version FROM ${entityClassName} WHERE (deleted = false or deleted is null) LIMIT ${limit} SKIP ${skip}`;

    const searchResult = await this.makeQuery(searchString);

    const docsVersionsData = [];
    for (let documentData of searchResult) {
        docsVersionsData.push({
            "objectId": documentData.objectId,
            "version": documentData.version
        });
    }

    return docsVersionsData;
}

OrientDBApp.prototype.getDocsVersionsSumByClassName = async function(entityClassName) {
    if (!entityClassName) {
        throw new Error("entityClassName is not defined");
    }

    const searchString = "SELECT sum(@version) FROM " + entityClassName + " WHERE (deleted = false or deleted is null)";

    const [searchResult] = await this.makeQuery(searchString);
    return searchResult.sum;
}

module.exports.OrientDBApp = OrientDBApp;