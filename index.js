var util = require('util');
var Q = require('q');
var mongojs = require('mongojs');
var pmongo = require('promised-mongo');

// *****************************************************************************
//constructors and factories

//tpmongo - so you don't wipe out historical data
var tpmongo = function(connectionString, collections, configOverride) {
    db = pmongo(connectionString, collections);

    //override each collection to use the mongoRep version
    collections.forEach(function(collectionName) {
        db[collectionName].drop();
        db[collectionName] = new temporalCollection(db[collectionName], configOverride);
        db[collectionName].cleanLocks();
    });

    return db;
};

var temporalCollection = function(collectionToUse, collectionConfig) {
    this.collection = collectionToUse;

    //default settings
    this.config = collectionConfig || {};
    this.config._retryAttempts = this.config._retryAttempts || 10;
    this.config._retryMilliseconds = this.config._retryMilliseconds || 10;
    this.config._retryFactor = this.config._retryFactor || 2; 
    this.config._maxDate = this.config._maxDate || new Date("2999-01-01T00:00:00.000Z");
};

var tpmongoError = function(errorCode, errorMessage) {
    repo.log("Throwing Error: " + errorMessage);
    return {
        success: false,
        errorCode: errorCode,
        errorMessage: errorMessage
    };
};
// *****************************************************************************

// *****************************************************************************
//bypass methods
temporalCollection.prototype.insertRaw = function(doc, options) {
    var repo = this;
    return repo.collection.insert(doc, options)
    .then(function() {
        return true;
    }, function(err) {
        return Q.reject(repo.tpmongoError("INSERT_ERROR", 
                        "The insert failed: " + JSON.stringify(err)));
    });
};

temporalCollection.prototype.updateRaw = function(query, docUpdate, options) {
    return this.collection.update(query, docUpdate, options);
};
// *****************************************************************************

// here we give users the ability to call save when doing an insert or an update
temporalCollection.prototype.save = function(doc) {
    if(doc._id === undefined) {
        return this.insert(doc);
    }
    else {
        return this.update(doc);
    }
};

temporalCollection.prototype.insert = function(doc, options) {
    var trackingId = mongojs.ObjectId();

    doc._id = trackingId;
    doc._trackingId = trackingId; 
    doc._current = 1;
    doc._startDate = this.getCurrentDate();
    doc._endDate = this.config._maxDate;
    return this.insertRaw(doc, options);
};

temporalCollection.prototype.insertClone = function(sourceDocument, currentDate, transactionId) {
    var repo = this;
    var clone = sourceDocument;
    clone._id = mongojs.ObjectId();

    delete clone._transactionId;
    delete clone._tranIds;

    clone._current = -1;
    clone._startDate = currentDate;
    clone._sourceTransactionId = transactionId;
    return repo.insertRaw(clone, {writeConcern: "majority"});
};

temporalCollection.prototype.update = function(query, docUpdate, options, transactionId, operationId, attempt) {
    var repo = this;
    var currentDate = repo.getCurrentDate();
    var _collection = repo.collection;

    transactionId = transactionId || mongojs.ObjectId();
    attempt = (attempt || 0) + 1;

    var queryForCurrent = util._extend({_current: 1}, query);
    var queryForCurrentUnlocked = util._extend({_locked: { $exists: false}}, queryForCurrent);
    var queryForCurrentByTransaction = util._extend({_transactionId: transactionId}, queryForCurrent);
    var optionsForLocking = util._extend({writeConcern: "majority"}, options || {});
    var lockedDocumentCount = 0;

    repo.info("Starting Phase 1");
    // PHASE 1, find and lock the docs to clone
    return repo.waitForUnlock(queryForCurrent)
    .then(function() {
        return _collection.update(queryForCurrentUnlocked, 
            {
                $set: {_locked: 1, _transactionId: transactionId}, 
                $push: {_tranIds: {_transactionId: transactionId}}
            }, 
                optionsForLocking);
    })
    .then(function(updateResult) {
        lockedDocumentCount = updateResult.n;
        //get all the documents to clone
        return _collection.find(queryForCurrent).toArray();
    })
    .then(function(sourceDocuments) {
        // *****************************************************************************
        //if the find pulls a different number of records, then two updates were attempted at the same time
        // retry and return error if necessary

        repo.info(JSON.stringify(sourceDocuments, 4));
        repo.info("sourceDocuments.length: " + sourceDocuments.length);
        repo.info("lockedDocumentCount: " + lockedDocumentCount);
        if(sourceDocuments.length != lockedDocumentCount) {
            //rollback locked docs
            return _collection.update({_transactionId: transactionId}, repo.getRollbackUpdate(transactionId), optionsForLocking)
            .then(function() {
                if(attempt > repo.config._retryAttempts) {
                    return Q.reject(repo.tpmongoError("DEADLOCK_DETECTED", 
                        "Two concurrent transactions were detected."));
                } else {
                    var nextDelay = repo.getNextDelay(attempt);

                    repo.info("DEADLOCK_DETECTED: attempt: " + attempt + ", nextDelay:" + nextDelay);
                    
                    return Q.delay(nextDelay)
                    .then(function() {
                        //retry
                        return repo.update.call(repo, query, docUpdate, options, transactionId, operationId, attempt);    
                    });
                }
            }, function(err) {
                return Q.reject(repo.tpmongoError("DEADLOCK_DETECTED_LOCK_NOT_RELEASED", 
                    "Two concurrent transactions were detected, and the lock could not be released: " + JSON.stringify(err)));
            });
        } else {
            repo.info("Starting Phase 2");
            // PHASE 2, insert clone - trackingId propogates
            //    there is no lock on this record
            var insertClonePromises = [];
            for(var sourceDocumentIter = 0; sourceDocumentIter < sourceDocuments.length; sourceDocumentIter++) {
                insertClonePromises.push(repo.insertClone(sourceDocuments[sourceDocumentIter], currentDate, transactionId));
            }

            return Q.all(insertClonePromises)
            .then(function() {
                repo.info("Starting Phase 3");
                // PHASE 3, update clone
                repo.addUnsetterToUpdate(docUpdate, "_locked");
                repo.addUnsetterToUpdate(docUpdate, "_sourceTransactionId");
                repo.addSetterToUpdate(docUpdate, "_current", 1);
                var cloneSelector = {_sourceTransactionId: transactionId};

                return _collection.update(cloneSelector, docUpdate, {multi: true, writeConcern: "majority"});
            }, function(err) {
                //rollback locked docs
                return _collection.update({_transactionId: transactionId}, repo.getRollbackUpdate(transactionId), optionsForLocking)
                .then(function() {
                    return Q.reject(repo.tpmongoError("CLONE_INSERTS_FAILED", 
                        "The clone insert(s) failed: "));
                }, function(err) {
                    return Q.reject(repo.tpmongoError("CLONE_INSERTS_FAILED_LOCK_NOT_RELEASED", 
                        "The clone insert(s) failed - lock not release: " + JSON.stringify(err)));
                });
            })
            .then(function() {
                repo.info("Starting Phase 4");
                // PHASE 4, unlock and archive original
                var unlockQuery = {_current: 1, _tranIds: { $elemMatch: { _transactionId: transactionId } } };
                var unlockUpdate = {
                                    $set: {
                                        _current: 0, 
                                        _endDate: currentDate
                                    },
                                    $pull: { 
                                        _tranIds: { _transactionId: transactionId } 
                                    }, 
                                    $unset: {
                                        _locked: "", 
                                        _transactionId: ""}
                                    };
                return _collection.update(unlockQuery, unlockUpdate, {multi:true})
                .then(function(updateResult) {
                    repo.info("PROCESS COMPLETED: " + updateResult.n);
                    return Q.when(operationId);
                }, function(err) {
                    return Q.reject(repo.tpmongoError("LOCK_RELEASE_FAILED", 
                        "The lock release failed: " + JSON.stringify(err)));
                });
            }, function(err) {
                return Q.reject(repo.tpmongoError("PROCESS_FAILED", 
                    "The update process failed: " + JSON.stringify(err)));
            });
        }
        // *****************************************************************************
    })
    .fail(function(err) {
        return Q.reject(err);
    });
};

temporalCollection.prototype.findAndModify = function(query, docUpdate, operationId, attempt) {
    var repo = this;
    var currentDate = repo.getCurrentDate();
    var _collection = repo.collection;
    var currentQuery = util._extend({_current: 1}, query);
    attempt = attempt || 0;
    return _collection.findAndModifyEx({query: currentQuery, update: {$set: {_locked: 1}}})
    .then(function(result) {
        if(result.result && result.result._locked === undefined) {
            var clone = result.result;
            var originalId = clone._id;
            clone._id = mongojs.ObjectId();
            clone._current = -1;
            clone._startDate = currentDate;
            return repo.insertRaw(clone)
            .then(function() {
                repo.addUnsetterToUpdate(docUpdate, "_locked");
                repo.addSetterToUpdate(docUpdate, "_current", 1);
                var cloneSelector = {_id: clone._id};
                return repo.updateRaw(cloneSelector, docUpdate)
            }, function(err) {
                return Q.reject(repo.tpmongoError("CLONE_INSERT_FAILED", 
                    "The clone insert failed: " + JSON.stringify(err)));
            })
            .then(function() {
                //phase 4, unlock and archive original
                var unlockQuery = {_id: originalId};
                var unlockUpdate = {$set: {_current: 0, _endDate: currentDate}, $unset: {_locked: ""}};
                return repo.updateRaw(unlockQuery, unlockUpdate)
                .then(function() {
                    return Q.when(operationId);
                }, function(err) {
                    return Q.reject(repo.tpmongoError("LOCK_RELEASE_FAILED", 
                        "The lock release failed: " + JSON.stringify(err)));
                });
                //finds will only look for current xor locked sort by 
            }, function(err) {
                return Q.reject(repo.tpmongoError("PROCESS_FAILED", 
                    "The clone/update process failed: " + JSON.stringify(err)));
            });
        }
        else if(result.result && result.result._locked !== undefined) {
            attempt++;
            if(attempt > repo.config._retryAttempts) {
                return Q.reject(repo.tpmongoError("EXCEEDED_RETRY_ATTEMPTS", 
                    "Document locked. " + repo.config._retryAttempts + " attempts exceeded."));
            }
            else {
                return Q.delay(repo.getNextDelay(attempt))
                .then(function() {
                    return repo.findAndModify(query, docUpdate, operationId, attempt);      
                });
            }
        }
        else {
            return Q.reject(repo.tpmongoError("NO_RESULTS", 
                "findAndModify returned no results."));
        }
    })
    .fail(function(err) {
        return Q.reject(repo.tpmongoError("FINDANDMODIFY_ERROR", 
                "findAndModify returned an error: " + JSON.stringify(err)))
    });
};

temporalCollection.prototype.remove = function(query, options) {
    var modifiedQuery = util._extend({_current: 1}, query);
    var multi = !((typeof options === "object") ? (options.justOne) : false);
    return this.collection.update(modifiedQuery, {$set: {_current:0, _endDate: this.getCurrentDate()}}, {multi: multi});
};

temporalCollection.prototype.find = function(query, projection) {
    var modifiedQuery = util._extend({_current: 1}, query);
    return this.collection.find(modifiedQuery);
};

temporalCollection.prototype.findOne = function(query, projection) {
    var modifiedQuery = util._extend({_current: 1}, query);
    return this.collection.findOne(modifiedQuery, projection);
};

temporalCollection.prototype.findByDate = function(query, date, projection) {
    var modifiedQuery = util._extend({}, query);
    modifiedQuery._startDate = {$lte : date};
    modifiedQuery._endDate = {$gt : date};
    return this.collection.find(modifiedQuery, projection);
};

// *****************************************************************************
//lock helpers
temporalCollection.prototype.getLockedDocumentCount = function(query) {
    var repo = this;
    var queryForLocks = util._extend({_locked: 1}, query);
    return repo.collection.count(queryForLocks)
    .then(function(docCount) {
        return docCount;
    })
    .fail(function(err) {
        return Q.reject(repo.tpmongoError("LOCK_CHECK_FAILED", 
                    "Check query object: " + JSON.stringify(err)));
    });
};

temporalCollection.prototype.waitForUnlock = function(query, attempt) {
    var repo = this;
    attempt = (attempt || 0) + 1;
    if(attempt > repo.config._retryAttempts) {
        return Q.reject(repo.tpmongoError("UNLOCK_WAIT_FAILURE", 
                    "Could not obtain lock."));
    } else {
        return repo.getLockedDocumentCount(query)
        .then(function(countInfo) {
            if(countInfo.docCount > 0) {
                var nextDelay = repo.getNextDelay(attempt);
                return Q.delay(nextDelay)
                .then(function() {
                    return repo.waitForUnlock(query, attempt);    
                })
            } else {
                return Q.when(true);
            }
        })
        .fail(function(err) {
            return Q.reject(err);
        });
    } 
};
// *****************************************************************************

// *****************************************************************************
//helpers
temporalCollection.prototype.addUnsetterToUpdate = function(update, propertyToUnset) {
    update.$unset = update.$unset || {};
    update.$unset[propertyToUnset] = "";
};

temporalCollection.prototype.addSetterToUpdate = function(update, propertyToSet, value) {
    update.$set = update.$set || {};
    update.$set[propertyToSet] = value;
};

temporalCollection.prototype.getCurrentDate = function() {
    return new Date();
};

temporalCollection.prototype.info = function(logText) {
    //console.log(logText);
};

temporalCollection.prototype.log = function(logText) {
    console.log(logText);
};

temporalCollection.prototype.getRollbackUpdate = function(transactionId) {
    return {
        $unset: {_transactionId: "", _locked: ""}, 
        $pull: { _tranIds: { $elemMatch: { _transactionId: transactionId } } }
    };
};

temporalCollection.prototype.getNextDelay = function(attempt) {
    var randomInt = function(low, high) {
        return Math.floor(Math.random() * (high - low) + low);
    };

    var delay = this.config._retryMilliseconds * Math.pow(this.config._retryFactor, attempt) + randomInt(5, 150);
    return delay;
};

// *****************************************************************************

// *****************************************************************************
//healthy repo
temporalCollection.prototype.cleanLocks = function() {
    var repo = this;
    var _collection = repo.collection;

    var cleaningPromises = [];

    var removeOrphanedCloneQuery = {};
    removeOrphanedCloneQuery._current = -1;
    cleaningPromises.push(_collection.remove(removeOrphanedCloneQuery));

    var unlockCurrentDocumentsQuery = {};
    unlockCurrentDocumentsQuery._current = 1;
    unlockCurrentDocumentsQuery._locked = 1;
    cleaningPromises.push(_collection.update(unlockCurrentDocumentsQuery, {$unset: {_locked: ""}}), {multi: true});

    var removeAllTranIdsQuery = {
        $exists: {tranIds: true}
    };
    cleaningPromises.push(_collection.update(removeAllTranIdsQuery, {$unset: {tranIds: ""}}), {multi: true});

    var removeAllTransactionIdsQuery = {
        $exists: {transactionId: true}
    };
    cleaningPromises.push(_collection.update(removeAllTransactionIdsQuery, {$unset: {transactionId: ""}}), {multi: true});

    return Q.all(cleaningPromises);
};
// *****************************************************************************

// *****************************************************************************
temporalCollection.prototype.aggregate = function() { return this.collection.aggregate.apply(this.collection, arguments) };
temporalCollection.prototype.createIndex = function() { return this.collection.createIndex.apply(this.collection, arguments) };
temporalCollection.prototype.drop = function() { return this.collection.drop.apply(this.collection, arguments) };
temporalCollection.prototype.dropIndex = function() { return this.collection.dropIndex.apply(this.collection, arguments) };
temporalCollection.prototype.dropIndexes = function() { return this.collection.dropIndexes.apply(this.collection, arguments) };
temporalCollection.prototype.ensureIndex = function() { return this.collection.ensureIndex.apply(this.collection, arguments) };
temporalCollection.prototype.getIndexes = function() { return this.collection.getIndexes.apply(this.collection, arguments) };
temporalCollection.prototype.isCapped = function() { return this.collection.isCapped.apply(this.collection, arguments) };
temporalCollection.prototype.reIndex = function() { return this.collection.reIndex.apply(this.collection, arguments) };
temporalCollection.prototype.runCommand = function() { return this.collection.runCommand.apply(this.collection, arguments) };
temporalCollection.prototype.stats = function() { return this.collection.stats.apply(this.collection, arguments) };
temporalCollection.prototype.toString = function() { return this.collection.toString.apply(this.collection, arguments) };
// *****************************************************************************

// *****************************************************************************
// *****************************************************************************
module.exports = tpmongo;
// *****************************************************************************