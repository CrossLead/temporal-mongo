var util = require('util');
var Q = require('q');
var pmongo = require('promised-mongo');

// *****************************************************************************
//constructors and factories

var TemporalMongo = function(connectionString, collections, configOverride) {
  db = pmongo(connectionString, collections);

  //override each collection to use the mongoRep version
  collections.forEach(function(collectionName) {
    //db[collectionName].drop();
    db[collectionName].ensureIndex({_current: 1});
    db[collectionName].ensureIndex({_transIds: 1});
    db[collectionName].ensureIndex({_current: 1, _transIds: 1});
    db[collectionName] = new TemporalCollection(db[collectionName], configOverride);

    db[collectionName].cleanLocks();
  });

  return db;
};

var TemporalCollection = function(collectionToUse, collectionConfig) {
  this.collection = collectionToUse;

  //default settings
  this.config = collectionConfig || {};
  this.config._retryAttempts = this.config._retryAttempts || 20;
  this.config._retryMilliseconds = this.config._retryMilliseconds || 15;
  this.config._retryFactor = this.config._retryFactor || 5; 
  this.config._retryMaxDelay = 10000; 
  this.config._maxDate = this.config._maxDate || new Date("2999-01-01T00:00:00.000Z");

  this.numberOfUpdates=0;
  this.numberOfFindAndModifies=0;
};

TemporalCollection.prototype.TemporalMongoError = function(errorCode, errorMessage) {
  var repo = this;
  repo.info("Throwing Error: " + errorMessage);
  return {
    success: false,
    errorCode: errorCode,
    errorMessage: errorMessage
  };
};
// *****************************************************************************

// *****************************************************************************
//bypass methods
TemporalCollection.prototype.insertRaw = function(doc, options) {
  var repo = this;
  return repo.collection.insert(doc, options)
  .then(function() {
    return true;
  }, function(err) {
    return Q.reject(repo.TemporalMongoError("INSERT_ERROR", 
            "The insert failed: " + JSON.stringify(err)));
  });
};

TemporalCollection.prototype.updateRaw = function(query, docUpdate, options) {
  return this.collection.update(query, docUpdate, options);
};
// *****************************************************************************

TemporalCollection.prototype.save = function(doc, options) {
  if(doc._id === undefined) {
    return this.insert(doc, options);
  }
  else {
    return Q.reject(repo.TemporalMongoError("UNSUPPORTED_OPERATION", 
            "Cannot call save with _id"));
  }
};

TemporalCollection.prototype.insert = function(doc, options) {
  var rId = pmongo.ObjectId();

  doc._id = rId;
  doc._rId = rId; 
  doc._current = 1;
  doc._startDate = this.getCurrentDate();
  doc._endDate = this.config._maxDate;
  return this.insertRaw(doc, options);
};

TemporalCollection.prototype.insertClone = function(sourceDocument, currentDate, tranId) {
  var repo = this;
  var clone = sourceDocument;
  clone._id = pmongo.ObjectId();

  delete clone._tranIds;

  clone._current = -1;
  clone._startDate = currentDate;
  clone._cloneTranId = tranId;
  return repo.insertRaw(clone, {writeConcern: "majority"});
};

TemporalCollection.prototype.makeClone = function(sourceDocument, currentDate, tranId) {
  var clone = sourceDocument;
  clone._id = pmongo.ObjectId();

  delete clone._tranIds;

  clone._current = -1;
  clone._startDate = currentDate;
  clone._cloneTranId = tranId;
  return clone;
};

TemporalCollection.prototype.update = function(query, docUpdate, options, transactionId, attempt) {
  var repo = this;
  var currentDate = repo.getCurrentDate();
  var _collection = repo.collection;

  if (options && options.upsert) {
    return Q.reject(repo.TemporalMongoError("UNSUPPORTED_OPERATION", 
            "Upsert not supported."));
  }

  var tranId = transactionId || pmongo.ObjectId();
  attempt = (attempt || 0) + 1;

  var queryForCurrent = util._extend({_current: 1}, query);
  var queryForCurrentByTransaction = util._extend({"_tranIds.0": tranId}, queryForCurrent);
  var optionsForLocking = util._extend({writeConcern: "majority"}, options || {});
  var lockedDocumentCount = 0;

  repo.info("Starting Phase 1");
  return _collection.update(queryForCurrent, {$push: {_tranIds: tranId}}, optionsForLocking)
  .then(function(updateResult) {
    lockedDocumentCount = updateResult.n;
    return _collection.find(queryForCurrentByTransaction).toArray();
  })
  .then(function(sourceDocuments) {
    if(lockedDocumentCount === 0) return true;
    
    //if the find pulls a different number of records, then two updates were attempted at the same time
    if(sourceDocuments.length != lockedDocumentCount) {
      return _collection.update({"_tranIds": tranId}, {$pull: { _tranIds: tranId }}, optionsForLocking)
      .then(function() {
        if(attempt > repo.config._retryAttempts) {
          return Q.reject(repo.TemporalMongoError("DEADLOCK_DETECTED", 
            "Two concurrent transactions were detected."));
        } else {
          var nextDelay = repo.getNextDelay(attempt)
          return Q.delay(nextDelay)
          .then(function() {
            repo.info("Attempt: " + attempt + ", " + nextDelay);
            return repo.update.call(repo, query, docUpdate, options, tranId, attempt);  
          });
        }
      }, function(err) {
        return Q.reject(repo.TemporalMongoError("DEADLOCK_DETECTED_LOCK_NOT_RELEASED", 
            "Two concurrent transactions were detected, and the lock could not be released: " + JSON.stringify(err)));
      });      
    } else {
      repo.info("Starting Phase 2");
      // PHASE 2, insert clones - rId propogates
      var clones = [];
      for(var sourceDocumentIter = 0; sourceDocumentIter < sourceDocuments.length; sourceDocumentIter++) {
        clones.push(repo.makeClone(sourceDocuments[sourceDocumentIter], currentDate, tranId));
      }

      return _collection.insert(clones, {writeConcern: "majority"})
      .then(function() {
        repo.info("Starting Phase 3");
        // PHASE 3, update clone
        repo.addUnsetterToUpdate(docUpdate, "_cloneTranId");
        repo.addSetterToUpdate(docUpdate, "_current", 1);
        repo.info(JSON.stringify(docUpdate));
        return _collection.update({_cloneTranId: tranId}, docUpdate, {multi: true, writeConcern: "majority"});
      }, function(err) {
        //rollback locked docs
        return _collection.update({_tranIds: tranId}, {$pull: { _tranIds: tranId }}, {multi: true, writeConcern: "majority"})
        .then(function() {
          return Q.reject(repo.TemporalMongoError("CLONE_INSERTS_FAILED", 
            "The clone insert(s) failed: "));
        }, function(err) {
          return Q.reject(repo.TemporalMongoError("CLONE_INSERTS_FAILED_LOCK_NOT_RELEASED", 
            "The clone insert(s) failed - lock not release: " + JSON.stringify(err)));
        });
      })
      .then(function() {
        repo.info("Starting Phase 4");
        // PHASE 4, unlock and archive original
        var unlockingQuery = {_current: 1, "_tranIds.0": tranId };
        var unlockingUpdate = {$set: {_current: 0, _endDate: currentDate}, $pull: {_tranIds: tranId}};
        return _collection.update(unlockingQuery, unlockingUpdate, {multi: true, writeConcern: "majority"})
        .then(function(updateResult) {
          repo.numberOfUpdates++;
          repo.info("Update Count: " + repo.numberOfUpdates);
          return Q.when(updateResult);
        }, function(err) {
          return Q.reject(repo.TemporalMongoError("LOCK_RELEASE_FAILED", 
            "The lock release failed: " + JSON.stringify(err)));
        });
      }, function(err) {
        return Q.reject(repo.TemporalMongoError("PROCESS_FAILED", 
          "The update process failed: " + JSON.stringify(err)));
      });
    }
    // *****************************************************************************
  })
  .fail(function(err) {
    return Q.reject(err);
  });
};

TemporalCollection.prototype.findAndModify = function(query, docUpdate, attempt) {
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
      clone._id = pmongo.ObjectId();
      clone._current = -1;
      clone._startDate = currentDate;
      return repo.insertRaw(clone)
      .then(function() {
        repo.addUnsetterToUpdate(docUpdate, "_locked");
        repo.addSetterToUpdate(docUpdate, "_current", 1);
        return repo.updateRaw({_id: clone._id}, docUpdate);
      }, function(err) {
        return Q.reject(repo.TemporalMongoError("CLONE_INSERT_FAILED", 
          "The clone insert failed: " + JSON.stringify(err)));
      })
      .then(function() {
        //phase 4, unlock and archive original
        var unlockQuery = {_id: originalId};
        var unlockUpdate = {$set: {_current: 0, _endDate: currentDate}, $unset: {_locked: ""}};
        return repo.updateRaw(unlockQuery, unlockUpdate)
        .then(function(updateResult) {
          return Q.when(updateResult);
        }, function(err) {
          return Q.reject(repo.TemporalMongoError("LOCK_RELEASE_FAILED", 
            "The lock release failed: " + JSON.stringify(err)));
        });
        //finds will only look for current xor locked sort by 
      }, function(err) {
        return Q.reject(repo.TemporalMongoError("PROCESS_FAILED", 
          "The clone/update process failed: " + JSON.stringify(err)));
      });
    }
    else if(result.result && result.result._locked !== undefined) {
      attempt++;
      if(attempt > repo.config._retryAttempts) {
        return Q.reject(repo.TemporalMongoError("EXCEEDED_RETRY_ATTEMPTS", 
          "Document locked. " + repo.config._retryAttempts + " attempts exceeded."));
      }
      else {
        return Q.delay(repo.getNextDelay(attempt))
        .then(function() {
          return repo.findAndModify(query, docUpdate, attempt);    
        });
      }
    }
    else {
      return Q.reject(repo.TemporalMongoError("NO_RESULTS", 
        "findAndModify returned no results."));
    }
  })
  .fail(function(err) {
    return Q.reject(repo.TemporalMongoError("FINDANDMODIFY_ERROR", 
        "findAndModify returned an error: " + JSON.stringify(err)))
  });
};

TemporalCollection.prototype.remove = function(query, options) {
  var modifiedQuery = util._extend({_current: 1}, query);
  var multi = !((typeof options === "object") ? (options.justOne) : false);
  return this.collection.update(modifiedQuery, {$set: {_current:0, _endDate: this.getCurrentDate()}}, {multi: multi});
};

TemporalCollection.prototype.find = function(query, projection) {
  var modifiedQuery = util._extend({_current: 1}, query);
  return this.collection.find(modifiedQuery);
};

TemporalCollection.prototype.findOne = function(query, projection) {
  var modifiedQuery = util._extend({_current: 1}, query);
  return this.collection.findOne(modifiedQuery, projection);
};

TemporalCollection.prototype.findByDate = function(query, date, projection) {
  var modifiedQuery = util._extend({}, query);
  modifiedQuery._startDate = {$lte : date};
  modifiedQuery._endDate = {$gt : date};
  return this.collection.find(modifiedQuery, projection);
};

// *****************************************************************************
//helpers
TemporalCollection.prototype.addUnsetterToUpdate = function(update, propertyToUnset) {
  update.$unset = update.$unset || {};
  update.$unset[propertyToUnset] = "";
};

TemporalCollection.prototype.addSetterToUpdate = function(update, propertyToSet, value) {
  update.$set = update.$set || {};
  update.$set[propertyToSet] = value;
};

TemporalCollection.prototype.getCurrentDate = function() {
  return new Date();
};

TemporalCollection.prototype.info = function(logText) {
  //console.log(logText);
};

TemporalCollection.prototype.log = function(logText) {
  console.log(logText);
};

TemporalCollection.prototype.getNextDelay = function(attempt) {
  var randomInt = function(low, high) {
    return Math.floor(Math.random() * (high - low) + low);
  };
  var delay = this.config._retryMilliseconds * this.config._retryFactor * attempt;// + randomInt(1, 20);
  if(delay > this.config._retryMaxDelay) delay = this.config._retryMaxDelay;
  return delay;
};
// *****************************************************************************

// *****************************************************************************
//healthy repo
TemporalCollection.prototype.cleanLocks = function() {
  var repo = this;
  var _collection = repo.collection;

  var cleaningPromises = [];

  var removeOrphanedCloneQuery = {};
  removeOrphanedCloneQuery._current = -1;
  cleaningPromises.push(_collection.remove(removeOrphanedCloneQuery, {writeConcern: "majority"})
  .then(function() {
      repo.info("Removed Orphaned Clones");
      return true;
    })
  );

  var unlockCurrentDocumentsQuery = {};
  unlockCurrentDocumentsQuery._current = 1;
  unlockCurrentDocumentsQuery._locked = 1;
  cleaningPromises.push(_collection.update(unlockCurrentDocumentsQuery, {$unset: {_locked: ""}}, {multi: true, writeConcern: "majority"})
  .then(function() {
      repo.info("Unset locked");
      return true;
    })
  );

  var removeAllTranIdsQuery = {
    _tranIds: {$exists: true}
  };
  cleaningPromises.push(_collection.update(removeAllTranIdsQuery, {$unset: {_tranIds: ""}}, {multi: true, writeConcern: "majority"})
    .then(function() {
      repo.info("Removed all tranIds");
      return true;
    })
  );

  return Q.all(cleaningPromises);
};
// *****************************************************************************

// *****************************************************************************
module.exports = TemporalMongo;
// *****************************************************************************