var util = require('util');
var Q = require('q');
var pmongo = require('promised-mongo');
var _ = require('lodash');

// *****************************************************************************
// Helpers
var CurrentStatus = {
  Archived: 0,
  Current: 1,
  InProgressClone: 2
};

// *****************************************************************************
// Constructors and pmongo pointers
var TemporalMongo = function(connectionString, collections, collectionConfig) {
  var db = pmongo(connectionString, collections);
  db.proxyCollections = {};

  _.forEach(collections, function(collectionName) {
    db.proxyCollections[collectionName] = db[collectionName];
    db[collectionName] = new TemporalCollection(collectionName, db[collectionName]._get, collectionConfig);
    db[collectionName].proxyCollection = db.proxyCollections[collectionName];
  });

  /**
   * @override
   * @param  {String} name Collection name
   * @param  {Object} [options] Options
   * @return {Collection}
   */
  db.collection = function(name/*, options*/) {
    if (db.proxyCollections[name]) {
      return db[name];
    } else {
      return pmongo.Database.prototype.collection.apply(this, arguments);
    }
  };

  return db;
};

// Maintain package API
_.assign(TemporalMongo, pmongo);

var TemporalCollection = function(collectionToUse, oncollection, collectionConfig) {
  if(!this.proxyCalled) {
    this.proxyCalled = true;
    pmongo.Collection.apply(this, arguments);
  }

  this.setConfig(collectionConfig);
};

TemporalCollection.prototype = Object.create(pmongo.Collection.prototype);
// *****************************************************************************

// *****************************************************************************
// bypass temporal

TemporalCollection.prototype.findRaw = function() {
  return this.proxyCollection.find.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.findOneRaw = function() {
  return this.proxyCollection.findOne.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.findAndModifyRaw = function() {
  return this.proxyCollection.findAndModify.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.findAndModifyExRaw = function() {
  return this.proxyCollection.findAndModifyEx.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.saveRaw = function() {
  return this.proxyCollection.save.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.insertRaw = function() {
  return this.proxyCollection.insert.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.updateRaw = function() {
  return this.proxyCollection.update.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.removeRaw = function() {
  return this.proxyCollection.remove.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.countRaw = function() {
  return this.proxyCollection.count.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.distinctRaw = function() {
  return this.proxyCollection.distinct.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.aggregateRaw = function() {
  return this.proxyCollection.aggregate.apply(this.proxyCollection, arguments);
};

TemporalCollection.prototype.mapReduceRaw = function() {
  return this.proxyCollection.mapReduce.apply(this.proxyCollection, arguments);
};
// *****************************************************************************

// *****************************************************************************
// config and error handling
TemporalCollection.prototype.setConfig = function(collectionConfig) {
  // take from the given settings or use default settings
  this.config = collectionConfig || {};
  this.config._retryAttempts = this.config._retryAttempts || 20;
  this.config._retryMilliseconds = this.config._retryMilliseconds || 15;
  this.config._retryFactor = this.config._retryFactor || 5;
  this.config._retryMaxDelay = 10000;
  this.config._maxDate = this.config._maxDate || new Date('2099-01-01 00:00:00.000Z');
};

TemporalCollection.prototype.TemporalMongoError = function(errorCode, errorMessage) {
  var _this = this;
  _this.info('Throwing Error: ' + errorMessage);
  return {
    success: false,
    errorCode: errorCode,
    errorMessage: errorMessage
  };
};
// *****************************************************************************

/**
 * For a `TemporalCollection`, `save()` semantics differ as follows:
 *  * Insertion vs update depends on `_rId` field vs `_id`
 * @param  {Object} doc document to save
 * @param  {Object} options primarily `writeConcern`
 * @return {Object} inserted or updated document
 */
TemporalCollection.prototype.save = function(doc, options) {
  var _this = this;
  if(doc._rId === undefined) {
    return _this.insert(doc, options);
  } else {
    const update = _.omit(doc, ['_id', '_rId']);
    return _this
      .findAndModify({
        query: { _rId: doc._rId },
        update: { $set: update },
        new: true,
        upsert: true
      })
      .then(function(result) {
        // Match promised-mongo save() format, which is just the doc
        return result[0];
      });
  }
};

TemporalCollection.prototype.aggregate = function(pipeline, options) {
  var _this = this;
  var modifiedPipeline = util._extend({}, pipeline);
  modifiedPipeline.$match = pipeline.$match || {};
  modifiedPipeline.$match._current = CurrentStatus.Current;
  return _this.aggregateRaw(modifiedPipeline, options);
};

TemporalCollection.prototype.aggregateByDate = function(pipeline, options, date) {
  var _this = this;
  var modifiedPipeline = util._extend({}, pipeline);
  modifiedPipeline.$match = pipeline.$match || {};
  modifiedPipeline.$match._startDate = {$lte : date};
  modifiedPipeline.$match._endDate = {$gt : date};
  return _this.aggregateRaw(modifiedPipeline, options);
};

TemporalCollection.prototype.count = function(query) {
  var _this = this;
  var modifiedQuery = util._extend({}, query);
  modifiedQuery._current = CurrentStatus.Current;
  return _this.countRaw(modifiedQuery);
};

TemporalCollection.prototype.countByDate = function(query, date) {
  var _this = this;
  var modifiedQuery = util._extend({}, query);
  modifiedQuery._startDate = {$lte : date};
  modifiedQuery._endDate = {$gt : date};
  return _this.countRaw(modifiedQuery);
};

TemporalCollection.prototype.distinct = function(field, query) {
  var _this = this;
  var modifiedQuery = util._extend({}, query);
  modifiedQuery._current = CurrentStatus.Current;
  return _this.distinctRaw(field, modifiedQuery);
};

TemporalCollection.prototype.distinctByDate = function(field, query, date) {
  var _this = this;
  var modifiedQuery = util._extend({}, query);
  modifiedQuery._startDate = {$lte : date};
  modifiedQuery._endDate = {$gt : date};
  return _this.distinctRaw(field, modifiedQuery);
};

TemporalCollection.prototype.mapReduce = function(map, reduce, options) {
  var _this = this;
  var modifiedOptions = util._extend({}, options);
  modifiedOptions.query = modifiedOptions.query || {};
  modifiedOptions.query._current = CurrentStatus.Current;
  return _this.mapReduceRaw(map, reduce, modifiedOptions);
};

TemporalCollection.prototype.mapReduceByDate = function(map, reduce, options, date) {
  var _this = this;
  var modifiedOptions = util._extend({}, options);
  modifiedOptions.query = modifiedOptions.query || {};
  modifiedOptions.query._startDate = {$lte : date};
  modifiedOptions.query._endDate = {$gt : date};
  return _this.mapReduceRaw(map, reduce, modifiedOptions);
};

TemporalCollection.prototype.insert = function(doc, options) {
  doc._rId = doc._rId || pmongo.ObjectId();
  doc._id = doc._rId; 
  doc._current = CurrentStatus.Current;
  doc._startDate = this.getCurrentDate();
  doc._endDate = this.config._maxDate;
  return this.insertRaw(doc, options);
};

TemporalCollection.prototype.insertClone = function(sourceDocument, currentDate, tranId) {
  var _this = this;
  var clone = sourceDocument;
  clone._id = pmongo.ObjectId();

  delete clone._tranIds;

  clone._current = CurrentStatus.InProgressClone;
  clone._startDate = currentDate;
  clone._cloneTranId = tranId;
  return _this.insertRaw(clone, {writeConcern: 'majority'});
};

TemporalCollection.prototype.makeClone = function(sourceDocument, currentDate, tranId) {
  var clone = sourceDocument;
  clone._id = pmongo.ObjectId();

  delete clone._tranIds;

  clone._current = CurrentStatus.InProgressClone;
  clone._startDate = currentDate;
  clone._endDate = this.config._maxDate;
  clone._cloneTranId = tranId;
  return clone;
};

TemporalCollection.prototype.update = function(query, docUpdate, options, transactionId, attempt) {
  var _this = this;
  var currentDate = _this.getCurrentDate();

  var tranId = transactionId || pmongo.ObjectId();
  attempt = (attempt || 0) + 1;

  var queryForCurrent = util._extend({_current: CurrentStatus.Current}, query);
  var queryForCurrentByTransaction = util._extend({'_tranIds.0': tranId}, queryForCurrent);
  var optionsForLocking = util._extend({writeConcern: 'majority'}, options || {});
  var lockedDocumentCount = 0;

  _this.info('Starting Phase 1');
  return _this.updateRaw(queryForCurrent, {$push: {_tranIds: tranId}, $set: {_endDate: currentDate}}, optionsForLocking)
  .then(function(updateResult) {
    lockedDocumentCount = updateResult.n;
    return _this.findRaw(queryForCurrentByTransaction).toArray();
  })
  .then(function(sourceDocuments) {
    if(lockedDocumentCount === 0) {
      if(options && options.upsert === true) {
        var upsertId = pmongo.ObjectId();
        return _this.insert({_id: upsertId})
        .then(function() {
          return _this.updateRaw({_id: upsertId}, docUpdate);
        });
      } else {
        return true;
      }
    }
    
    //if the find pulls a different number of records, then two updates were attempted at the same time
    if(sourceDocuments.length !== lockedDocumentCount) {
      return _this.updateRaw({'_tranIds': tranId}, {$pull: { _tranIds: tranId }}, optionsForLocking)
      .then(function() {
        if(attempt > _this.config._retryAttempts) {
          return Q.reject(_this.TemporalMongoError('DEADLOCK_DETECTED', 
            'Two concurrent transactions were detected.'));
        } else {
          var nextDelay = _this.getNextDelay(attempt);
          return Q.delay(nextDelay)
          .then(function() {
            _this.info('Attempt: ' + attempt + ', ' + nextDelay);
            return _this.update.call(_this, query, docUpdate, options, tranId, attempt);  
          });
        }
      }, function(err) {
        return Q.reject(_this.TemporalMongoError('DEADLOCK_DETECTED_LOCK_NOT_RELEASED', 
            'Two concurrent transactions were detected, and the lock could not be released: ' + JSON.stringify(err)));
      });      
    } else {
      _this.info('Starting Phase 2');
      // PHASE 2, insert clones - rId propogates
      var clones = [];
      for(var sourceDocumentIter = 0; sourceDocumentIter < sourceDocuments.length; sourceDocumentIter++) {
        clones.push(_this.makeClone(sourceDocuments[sourceDocumentIter], currentDate, tranId));
      }

      return _this.insertRaw(clones, {writeConcern: 'majority'})
      .then(function() {
        _this.info('Starting Phase 3');
        // PHASE 3, update clone
        _this.info(JSON.stringify(docUpdate));
        return _this.updateRaw({_cloneTranId: tranId}, docUpdate, {multi: true, writeConcern: 'majority'});
      }, function(cloneInsertError) {
        //rollback locked docs
        console.log('tpmongo cloneInsertError - check unique indexes');
        console.log(cloneInsertError);
        return _this.updateRaw({_tranIds: tranId}, {$pull: { _tranIds: tranId }, $set: { _endDate: _this.config._maxDate }}, {multi: true, writeConcern: 'majority'})
        .then(function() {
          return Q.reject(_this.TemporalMongoError('CLONE_INSERTS_FAILED', 
            'The clone insert(s) failed: '));
        }, function(err) {
          return Q.reject(_this.TemporalMongoError('CLONE_INSERTS_FAILED_LOCK_NOT_RELEASED', 
            'The clone insert(s) failed - lock not release: ' + JSON.stringify(err)));
        });
      })
      .then(function() {
        _this.info('Starting Phase 4');
        // PHASE 4: simultaneously:
        // * unlock and archive originals
        // * set clones as current
        var unlockingQuery = {
          $or: [{
            '_tranIds.0': tranId
          }, {
            _cloneTranId: tranId
          }]
        };
        var unlockingUpdate = {
          $inc: { _current: -1 },
          $unset: { _cloneTranId: '' },
          $pull: { _tranIds: tranId }
        };
        return _this.updateRaw(unlockingQuery, unlockingUpdate, {multi: true, writeConcern: 'majority'})
        .then(function(updateResult) {
          if(updateResult.n !== 2*lockedDocumentCount) {
            return Q.reject(_this.TemporalMongoError('LOCK_RELEASE_FAILED',
              'Incorrect number of clones and current documents updated. Expected: ' + 2*lockedDocumentCount + '. Got: ' + updateResult.n));
          }
          // Remap the updated doc count since we updated all clones and currents in 1 step
          var result = _.omit(updateResult, 'n');
          result.n = lockedDocumentCount;
          return Q.when(result);
        }, function(err) {
          return Q.reject(_this.TemporalMongoError('LOCK_RELEASE_FAILED', 
            'The lock release failed: ' + JSON.stringify(err)));
        });
      }, function(err) {
        return Q.reject(_this.TemporalMongoError('PROCESS_FAILED', 
          'The update process failed: ' + JSON.stringify(err)));
      });
    }
    // *****************************************************************************
  })
  .fail(function(err) {
    return Q.reject(err);
  });
};

TemporalCollection.prototype.findAndModify = function(options, attempt) {
  var _this = this;
  var currentDate = _this.getCurrentDate();
  var currentQuery = util._extend({_current: CurrentStatus.Current}, options.query);
  var docUpdate = options.update;
  var sort = options.sort || {};
  attempt = attempt || 0;

  if (options && options.remove === true) {
    return Q.reject(_this.TemporalMongoError('UNSUPPORTED_OPERATION', 
            'Remove is not supported on findAndModify.'));
  }

  return _this.findAndModifyExRaw({query: currentQuery, sort: sort, update: {$set: {_locked: 1, _endDate: currentDate}}})
  .then(function(result) {
    if(result.result && result.result._locked === undefined) {
      var clone = result.result;
      var originalId = clone._id;
      clone._id = pmongo.ObjectId();
      clone._current = CurrentStatus.InProgressClone;
      clone._startDate = currentDate;
      clone._endDate = _this.config._maxDate;
      return _this.insertRaw(clone)
        .then(function() {
          _this.addUnsetterToUpdate(docUpdate, '_locked');
          return _this.updateRaw({_id: clone._id}, docUpdate);
        })
        .catch(function(err) {
          return Q.reject(_this.TemporalMongoError('CLONE_INSERT_FAILED', 
            'The clone insert failed: ' + JSON.stringify(err)));
        })
        .then(function() {
          //phase 4: simultaneously do following:
          // * unlock and archive original
          // * set clone as current
          var unlockQuery = {_id: {$in: [originalId, clone._id]}};
          var unlockUpdate = {$inc: {_current: -1}, $unset: {_locked: ''}};
          var unlockOpts = {multi: true, writeConcern: 'majority'};
          return _this.updateRaw(unlockQuery, unlockUpdate, unlockOpts)
            .then(function() {
              //mimic old api return type
              if(options.new) {
                return _this.findAndModifyRaw({query: {_id: clone._id}, update: {$unset: {fieldThatDoesNotExist:''}}});
              } else {
                return _this.findAndModifyRaw({query: {_id: originalId}, update: {$unset: {fieldThatDoesNotExist:''}}});
              }
            })
            .catch(function(err) {
              return Q.reject(_this.TemporalMongoError('LOCK_RELEASE_FAILED', 
                'The lock release failed: ' + JSON.stringify(err)));
            });
        })
        .catch(function(err) {
          return Q.reject(_this.TemporalMongoError('PROCESS_FAILED', 
            'The clone/update process failed: ' + JSON.stringify(err)));
        });
    }
    else if(result.result && result.result._locked !== undefined) {
      attempt++;
      if(attempt > _this.config._retryAttempts) {
        return Q.reject(_this.TemporalMongoError('EXCEEDED_RETRY_ATTEMPTS', 
          'Document locked. ' + _this.config._retryAttempts + ' attempts exceeded.'));
      }
      else {
        return Q.delay(_this.getNextDelay(attempt))
        .then(function() {
          return _this.findAndModify(options, attempt);    
        });
      }
    }
    else if (options.upsert) {
      var upsertId = currentQuery._rId || 
        (docUpdate.$setOnInsert && docUpdate.$setOnInsert._rId) ||
        (docUpdate.$set && docUpdate.$set._rId) ||
        pmongo.ObjectId();
      return _this.insert({_rId: upsertId})
        .then(function() {
          return _this.updateRaw({_rId: upsertId}, docUpdate);
        })
        .then(function() {
          // Match findAndModify() result format
          if(options.new) {
            return _this.findAndModifyRaw({query: {_rId: upsertId}, update: {$unset: {fieldThatDoesNotExist:''}}});
          } else {
            return null;
          }
        });
    }
    else {
      return Q.reject(_this.TemporalMongoError('NO_RESULTS', 
        'findAndModify returned no results.'));
    }
  })
  .fail(function(err) {
    return Q.reject(_this.TemporalMongoError('FINDANDMODIFY_ERROR', 
        'findAndModify returned an error: ' + JSON.stringify(err)));
  });
};

TemporalCollection.prototype.remove = function(query, options) {
  var _this = this;
  var modifiedQuery = util._extend({_current: CurrentStatus.Current}, query);
  var multi = !((typeof options === 'object') ? (options.justOne) : false);
  return _this.updateRaw(modifiedQuery, {$set: {_current:CurrentStatus.Archived, _endDate: this.getCurrentDate()}}, {multi: multi})
  .then(function(result) {
    return _.pick(result, ['n']);
  });
};

TemporalCollection.prototype.find = function(query, projection) {
  var _this = this;
  var modifiedQuery = util._extend({_current: CurrentStatus.Current}, query);
  return _this.findRaw(modifiedQuery, projection || {});
};

TemporalCollection.prototype.findOne = function(query, projection) {
  var _this = this;
  var modifiedQuery = util._extend({_current: CurrentStatus.Current}, query);
  if(projection) {
    return _this.findOneRaw(modifiedQuery, projection);
  }
  else
  {
    return _this.findOneRaw(modifiedQuery);
  }
};

TemporalCollection.prototype.findOneByDate = function(query, date, projection) {
  var _this = this;
  var modifiedQuery = util._extend({}, query);
  modifiedQuery._startDate = {$lte : date};
  modifiedQuery._endDate = {$gt : date};

  if(projection) {
    return _this.findOneRaw(modifiedQuery, projection);
  }
  else
  {
    return _this.findOneRaw(modifiedQuery);
  }
};

TemporalCollection.prototype.findByDate = function(query, date, projection) {
  var _this = this;
  var modifiedQuery = util._extend({}, query);
  modifiedQuery._startDate = {$lte : date};
  modifiedQuery._endDate = {$gt : date};

  if(projection) {
    return _this.findRaw(modifiedQuery, projection);
  }
  else
  {
    return _this.findRaw(modifiedQuery);
  }
};

// *****************************************************************************
//helpers
TemporalCollection.prototype.addUnsetterToUpdate = function(update, propertyToUnset) {
  update.$unset = update.$unset || {};
  update.$unset[propertyToUnset] = '';
};

TemporalCollection.prototype.addSetterToUpdate = function(update, propertyToSet, value) {
  update.$set = update.$set || {};
  update.$set[propertyToSet] = value;
};

TemporalCollection.prototype.getCurrentDate = function() {
  return new Date();
};

TemporalCollection.prototype.info = function(/* logText */) {
  //console.log(logText);
};

TemporalCollection.prototype.getNextDelay = function(attempt) {
  var delay = this.config._retryMilliseconds * this.config._retryFactor * attempt;
  if(delay > this.config._retryMaxDelay) {
    delay = this.config._retryMaxDelay;
  }
  return delay;
};
// *****************************************************************************

// *****************************************************************************
// maintain a healthy temporal collection

TemporalCollection.prototype.temporalize = function() {
  var _this = this;

  var batchCount = 0;
  var documentsPerBatch = 50;

  return _this.count({_rId: {$exists: true}})
  .then(function(alreadyTemporalizedCount) {
    if(alreadyTemporalizedCount > 0) {
      return Q.reject('Collection already temporalized.');
    } else {
      return _this.temporalizeBatch(pmongo.ObjectId('000000000000000000000000'), documentsPerBatch, batchCount);
    }
  })
  .catch(function(err) {
    return Q.reject('Error Temporalizing: ' + JSON.stringify(err));
  });
};

TemporalCollection.prototype.temporalizeBatch = function(lastId, documentsPerBatch, batchCount) {
  batchCount++;
  var _this = this;
  return _this.findRaw({_rId: {$exists: false}, _id: {$gt: lastId}}, {_id: 1}).sort({_id: 1}).limit(documentsPerBatch).toArray()
  .then(function(docsToTemporalize) {
    if(docsToTemporalize && docsToTemporalize.length > 0) {
      var updatePromises = [];
      for(var docIter = 0; docIter < docsToTemporalize.length; docIter++) {
        var startDate = new Date(parseInt(docsToTemporalize[docIter]._id.toString().slice(0,8), 16)*1000);
        var docId = docsToTemporalize[docIter]._id;
        updatePromises.push(_this.findAndModifyExRaw( { query: { _id:  docId}, update: { $set: { _current: CurrentStatus.Current, _rId: docId, _startDate: startDate, _endDate: _this.config._maxDate } }} ));
      }
      return Q.all(updatePromises)
      .then(function() {
        return _this.temporalizeBatch(docsToTemporalize[docsToTemporalize.length-1]._id, documentsPerBatch, batchCount);
      });
    } else {
      return true;
    }
  });
};

TemporalCollection.prototype.ensureIndexes = function() {
  var _this = this;

  const promises = [
    _this.ensureIndex({_current: CurrentStatus.Current}),
    _this.ensureIndex({_transIds: 1}),
    _this.ensureIndex({_current: CurrentStatus.Current, _transIds: 1}),
    _this.ensureIndex({_rId: 1}),
    // Must use runCommand until Mongo 3.1.8 releases (w/updated driver)
    _this.runCommand('createIndexes', {
      indexes: [{
        key: {
          _rId: 1,
          _current: 1
        },
        name: '__rId_1__current_1',
        unique: true,
        partialFilterExpression: {
          _current: 1
        }
      }]
    })
  ];
  
  return Promise.all(promises);
};

TemporalCollection.prototype.cleanLocks = function(cleanBeforeDate) {
  var _this = this;

  cleanBeforeDate = cleanBeforeDate || new Date();

  var cleaningPromises = [];

  var removeOrphanedCloneQuery = {
    _current: CurrentStatus.InProgressClone,
    _startDate: {
      $lt: cleanBeforeDate
    }
  };

  cleaningPromises.push(_this.removeRaw(removeOrphanedCloneQuery, {writeConcern: 'majority'})
    .then(function() {
      _this.info('Removed Orphaned Clones');
      return true;
    })
  );

  var fixEndDateQuery = {
    _current: CurrentStatus.Current,
    _endDate: {
      $ne: _this.config._maxDate
    },
    _startDate: {
      $lt: cleanBeforeDate
    }
  };

  var fixEndDateUpdate = {
    $set: {
      _endDate: _this.config._maxDate
    }
  };

  cleaningPromises.push(_this.updateRaw(fixEndDateQuery, fixEndDateUpdate, {multi: true, writeConcern: 'majority'})
    .then(function() {
      _this.info('Fixed End Dates');
      return true;
    })
  );

  var unlockCurrentDocumentsQuery = {
    _current: CurrentStatus.Current,
    _locked: 1,
    _startDate: {
      $lt: cleanBeforeDate
    }
  };

  cleaningPromises.push(_this.updateRaw(unlockCurrentDocumentsQuery, {$unset: {_locked: ''}, $set: {_endDate: _this.config._maxDate}}, {multi: true, writeConcern: 'majority'})
    .then(function() {
      _this.info('Unset locked');
      return true;
    })
  );

  var removeAllTranIdsQuery = {
    _tranIds: {
      $exists: true
    },
    _startDate: {
      $lt: cleanBeforeDate
    }
  };
  cleaningPromises.push(_this.updateRaw(removeAllTranIdsQuery, {$unset: {_tranIds: ''}}, {multi: true, writeConcern: 'majority'})
    .then(function() {
      _this.info('Removed all tranIds');
      return true;
    })
  );

  return Q.all(cleaningPromises);
};

// *****************************************************************************

// *****************************************************************************
TemporalMongo.TemporalCollection = TemporalCollection;
TemporalMongo.CurrentStatus = CurrentStatus;
module.exports = TemporalMongo;
// *****************************************************************************
