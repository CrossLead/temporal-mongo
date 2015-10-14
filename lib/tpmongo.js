import pmongo                  from 'promised-mongo';
import PromisedMongoDatabase   from 'promised-mongo/dist/Database';
import PromisedMongoCollection from 'promised-mongo/dist/Collection';

// *****************************************************************************
// Helpers
const CurrentStatus = {
  Archived: 0,
  Current: 1,
  InProgressClone: 2
};

/**
 * Exclude properties from an object
 *
 * @param  {Object} object to clone (other than exclude list)
 * @param  {Array | String} propery(ies) to exclude
 * @return {Object} Filtered object
 */
function omit(obj, exclude) {
  const excludeSet = new Set([].concat(exclude)),
        out = {};

  for (const prop in obj) {
    if (!excludeSet.has(prop)) {
      out[prop] = obj[prop];
    }
  }

  return out;
}

const omitList = ['_current'];
/**
 * Sanitize update parameters. For example, application should not be messing with
 * the _current flag in update() or findAndModify(). If they do, likely the document
 * will get left in unusable state since tmongo manipulates it for both the original
 * doc and to-be-archived clones
 *
 * @param  {Object} update parameter passed to update() or findAndModify()
 * @return {Object} sanitized update parameter
 */
function sanitizeUpdate(update) {
  return _(update)
    .omit(omitList) // root props (e.g. the doc itself)
    .mapValues((v, k) => {
      // $set, $setOnInsert
      if (v instanceof Object && (k === '$set' || k === '$setOnInsert')) {
        return omit(v, omitList);
      } else {
        return v;
      }
    })
    .value();
};


/**
 * Pause for <ms>
 *
 * @param {Integer} milliseconds to wait
 * @return Promise<undefined> promise resolving after delay
 */
function delay(ms = 0) {
  return new Promise(res => setTimeout(res, ms));
}

// *****************************************************************************
// Constructors and pmongo pointers
const TemporalMongo = function(connectionString, collections=[], collectionConfig={}) {
  const db = pmongo(connectionString, collections);
  db.proxyCollections = {};

  collections.forEach(name => {
    db.proxyCollections[name] = db[name];
    db[name] = new TemporalCollection(db, name, collectionConfig);
    db[name].proxyCollection = db.proxyCollections[name];
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
      return PromisedMongoDatabase.prototype.collection.apply(this, arguments);
    }
  };

  return db;
};

// Maintain package API
Object.assign(TemporalMongo, pmongo);

const TemporalCollection = function(db, name, collectionConfig) {
  if(!this.proxyCalled) {
    this.proxyCalled = true;
    PromisedMongoCollection.apply(this, arguments);
  }

  this.setConfig(collectionConfig);
};

TemporalCollection.prototype = Object.create(PromisedMongoCollection.prototype);
// *****************************************************************************

// *****************************************************************************
// bypass temporal

[
  'find',
  'findOne',
  'findAndModify',
  'save',
  'insert',
  'update',
  'remove',
  'count',
  'distinct',
  'aggregate',
  'mapReduce'
].forEach(method => {
  TemporalCollection.prototype[`${method}Raw`] = function(...args) {
    return this.proxyCollection[method](...args);
  };
});

// *****************************************************************************

// *****************************************************************************
// config and error handling
TemporalCollection.prototype.setConfig = function(collectionConfig) {
  // take from the given settings or use default settings
  this.config = Object.assign({
    _retryAttempts:       20,
    _retryMilliseconds:   15,
    _retryFactor:         5,
    _retryMaxDelay:       10000,
    _maxDate:             new Date('2099-01-01 00:00:00.000Z')
  }, collectionConfig);
};

TemporalCollection.prototype.TemporalMongoError = function(errorCode, errorMessage) {
  const _this = this;
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
TemporalCollection.prototype.save = async function(doc, options) {
  const { _rId } = doc;
  delete doc._id;
  delete doc._rId;

  if(_rId === undefined) {
    return this.insert(doc, options);
  } else {
    const result = await this.findAndModify({
      query:  { _rId },
      update: { $set: doc },
      new:    true,
      upsert: true
    });

    // Match promised-mongo save() format, which is just the doc
    return result.value;
  }
};

TemporalCollection.prototype.aggregate = function(pipeline, options) {
  const _this = this;
  const modifiedPipeline = Object.assign({}, pipeline);
  modifiedPipeline.$match = pipeline.$match || {};
  modifiedPipeline.$match._current = CurrentStatus.Current;
  return _this.aggregateRaw(modifiedPipeline, options);
};

TemporalCollection.prototype.aggregateByDate = function(pipeline, options, date) {
  const _this = this;
  const modifiedPipeline = Object.assign({}, pipeline);
  modifiedPipeline.$match = pipeline.$match || {};
  modifiedPipeline.$match._startDate = {$lte : date};
  modifiedPipeline.$match._endDate = {$gt : date};
  return _this.aggregateRaw(modifiedPipeline, options);
};

TemporalCollection.prototype.count = function(query) {
  const _this = this;
  const modifiedQuery = Object.assign({}, query);
  modifiedQuery._current = CurrentStatus.Current;
  return _this.countRaw(modifiedQuery);
};

TemporalCollection.prototype.countByDate = function(query, date) {
  const _this = this;
  const modifiedQuery = Object.assign({}, query);
  modifiedQuery._startDate = {$lte : date};
  modifiedQuery._endDate = {$gt : date};
  return _this.countRaw(modifiedQuery);
};

TemporalCollection.prototype.distinct = function(field, query) {
  const _this = this;
  const modifiedQuery = Object.assign({}, query);
  modifiedQuery._current = CurrentStatus.Current;
  return _this.distinctRaw(field, modifiedQuery);
};

TemporalCollection.prototype.distinctByDate = function(field, query, date) {
  const _this = this;
  const modifiedQuery = Object.assign({}, query);
  modifiedQuery._startDate = {$lte : date};
  modifiedQuery._endDate = {$gt : date};
  return _this.distinctRaw(field, modifiedQuery);
};

TemporalCollection.prototype.mapReduce = function(map, reduce, options) {
  const _this = this;
  const modifiedOptions = Object.assign({}, options);
  modifiedOptions.query = modifiedOptions.query || {};
  modifiedOptions.query._current = CurrentStatus.Current;
  return _this.mapReduceRaw(map, reduce, modifiedOptions);
};

TemporalCollection.prototype.mapReduceByDate = function(map, reduce, options, date) {
  const _this = this;
  const modifiedOptions = Object.assign({}, options);
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
  const _this = this;
  const clone = sourceDocument;
  clone._id = pmongo.ObjectId();

  delete clone._tranIds;

  clone._current = CurrentStatus.InProgressClone;
  clone._startDate = currentDate;
  clone._cloneTranId = tranId;
  return _this.insertRaw(clone, {writeConcern: 'majority'});
};

TemporalCollection.prototype.makeClone = function(sourceDocument, currentDate, tranId) {
  const clone = sourceDocument;
  clone._id = pmongo.ObjectId();

  delete clone._tranIds;

  clone._current = CurrentStatus.InProgressClone;
  clone._startDate = currentDate;
  clone._endDate = this.config._maxDate;
  clone._cloneTranId = tranId;
  return clone;
};

TemporalCollection.prototype.update = function(query, docUpdate, options, transactionId, attempt) {
  const _this = this;
  const currentDate = _this.getCurrentDate();

  const tranId = transactionId || pmongo.ObjectId();
  attempt = (attempt || 0) + 1;

  const queryForCurrent = Object.assign({_current: CurrentStatus.Current}, query);
  const queryForCurrentByTransaction = Object.assign({'_tranIds.0': tranId}, queryForCurrent);
  const optionsForLocking = Object.assign({writeConcern: 'majority'}, options || {});
  const lockedDocumentCount = 0;
  const sanitizedDocUpdate = sanitizeUpdate(docUpdate);

  _this.info('Starting Phase 1');
  return _this.updateRaw(queryForCurrent, {$push: {_tranIds: tranId}, $set: {_endDate: currentDate}}, optionsForLocking)
  .then(function(updateResult) {
    lockedDocumentCount = updateResult.n;
    return _this.findRaw(queryForCurrentByTransaction).toArray();
  })
  .then(function(sourceDocuments) {
    if(lockedDocumentCount === 0) {
      if(options && options.upsert === true) {
        const upsertId = pmongo.ObjectId();
        return _this.insert({_id: upsertId})
        .then(function() {
          return _this.updateRaw({_id: upsertId}, sanitizedDocUpdate);
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
          return Promise.reject(_this.TemporalMongoError('DEADLOCK_DETECTED',
            'Two concurrent transactions were detected.'));
        } else {
          const nextDelay = _this.getNextDelay(attempt);
          return delay(nextDelay)
          .then(function() {
            _this.info('Attempt: ' + attempt + ', ' + nextDelay);
            return _this.update.call(_this, query, sanitizedDocUpdate, options, tranId, attempt);
          });
        }
      }, function(err) {
        return Promise.reject(_this.TemporalMongoError('DEADLOCK_DETECTED_LOCK_NOT_RELEASED',
            'Two concurrent transactions were detected, and the lock could not be released: ' + JSON.stringify(err)));
      });
    } else {
      _this.info('Starting Phase 2');
      // PHASE 2, insert clones - rId propogates
      const clones = [];
      for(let sourceDocumentIter = 0; sourceDocumentIter < sourceDocuments.length; sourceDocumentIter++) {
        clones.push(_this.makeClone(sourceDocuments[sourceDocumentIter], currentDate, tranId));
      }

      return _this.insertRaw(clones, {writeConcern: 'majority'})
      .then(function() {
        _this.info('Starting Phase 3');
        // PHASE 3, update clone
        _this.info(JSON.stringify(sanitizedDocUpdate));
        return _this.updateRaw({_cloneTranId: tranId}, sanitizedDocUpdate, {multi: true, writeConcern: 'majority'});
      }, function(cloneInsertError) {
        //rollback locked docs
        console.log('tpmongo cloneInsertError - check unique indexes');
        console.log(cloneInsertError);
        return _this.updateRaw({_tranIds: tranId}, {$pull: { _tranIds: tranId }, $set: { _endDate: _this.config._maxDate }}, {multi: true, writeConcern: 'majority'})
        .then(function() {
          return Promise.reject(_this.TemporalMongoError('CLONE_INSERTS_FAILED',
            'The clone insert(s) failed: '));
        }, function(err) {
          return Promise.reject(_this.TemporalMongoError('CLONE_INSERTS_FAILED_LOCK_NOT_RELEASED',
            'The clone insert(s) failed - lock not release: ' + JSON.stringify(err)));
        });
      })
      .then(function() {
        _this.info('Starting Phase 4');
        // PHASE 4: simultaneously:
        // * unlock and archive originals
        // * set clones as current
        const unlockingQuery = {
          $or: [{
            '_tranIds.0': tranId
          }, {
            _cloneTranId: tranId
          }]
        };
        const unlockingUpdate = {
          $inc: { _current: -1 },
          $unset: { _cloneTranId: '' },
          $pull: { _tranIds: tranId }
        };
        return _this.updateRaw(unlockingQuery, unlockingUpdate, {multi: true, writeConcern: 'majority'})
        .then(function(updateResult) {
          if(updateResult.n !== 2*lockedDocumentCount) {
            return Promise.reject(_this.TemporalMongoError('LOCK_RELEASE_FAILED',
              'Incorrect number of clones and current documents updated. Expected: ' + 2*lockedDocumentCount + '. Got: ' + updateResult.n));
          }
          // Remap the updated doc count since we updated all clones and currents in 1 step
          const result = updateResult;
          result.n = lockedDocumentCount;
          return Promise.resolve(result);
        }, function(err) {
          return Promise.reject(_this.TemporalMongoError('LOCK_RELEASE_FAILED',
            'The lock release failed: ' + JSON.stringify(err)));
        });
      }, function(err) {
        return Promise.reject(_this.TemporalMongoError('PROCESS_FAILED',
          'The update process failed: ' + JSON.stringify(err)));
      });
    }
    // *****************************************************************************
  })
  .catch(function(err) {
    return Promise.reject(err);
  });
};

TemporalCollection.prototype.findAndModify = function(options, attempt) {
  const _this = this;
  const currentDate = _this.getCurrentDate();
  const currentQuery = Object.assign({_current: CurrentStatus.Current}, options.query);
  const docUpdate = sanitizeUpdate(options.update);
  const sort = options.sort || {};
  attempt = attempt || 0;

  if (options && options.remove === true) {
    return Promise.reject(_this.TemporalMongoError('UNSUPPORTED_OPERATION',
            'Remove is not supported on findAndModify.'));
  }

  return _this.findAndModifyRaw({query: currentQuery, sort: sort, update: {$set: {_locked: 1, _endDate: currentDate}}})
  .then(function(result) {
    if(result.value && result.value._locked === undefined) {
      const clone = result.value;
      const originalId = clone._id;
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
          return Promise.reject(_this.TemporalMongoError('CLONE_INSERT_FAILED',
            'The clone insert failed: ' + JSON.stringify(err)));
        })
        .then(function() {
          //phase 4: simultaneously do following:
          // * unlock and archive original
          // * set clone as current
          const unlockQuery = {_id: {$in: [originalId, clone._id]}};
          const unlockUpdate = {$inc: {_current: -1}, $unset: {_locked: ''}};
          const unlockOpts = {multi: true, writeConcern: 'majority'};
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
              return Promise.reject(_this.TemporalMongoError('LOCK_RELEASE_FAILED',
                'The lock release failed: ' + JSON.stringify(err)));
            });
        })
        .catch(function(err) {
          return Promise.reject(_this.TemporalMongoError('PROCESS_FAILED',
            'The clone/update process failed: ' + JSON.stringify(err)));
        });
    }
    else if(result.value && result.value._locked !== undefined) {
      attempt++;
      if(attempt > _this.config._retryAttempts) {
        return Promise.reject(_this.TemporalMongoError('EXCEEDED_RETRY_ATTEMPTS',
          'Document locked. ' + _this.config._retryAttempts + ' attempts exceeded.'));
      }
      else {
        return delay(_this.getNextDelay(attempt))
        .then(function() {
          return _this.findAndModify(options, attempt);
        });
      }
    }
    else if (options.upsert) {
      const upsertId = currentQuery._rId ||
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
      return Promise.reject(_this.TemporalMongoError('NO_RESULTS',
        'findAndModify returned no results.'));
    }
  })
  .catch(function(err) {
    return Promise.reject(_this.TemporalMongoError('FINDANDMODIFY_ERROR',
        'findAndModify returned an error: ' + JSON.stringify(err)));
  });
};

TemporalCollection.prototype.remove = function(query, options) {
  const _this = this;
  const modifiedQuery = Object.assign({_current: CurrentStatus.Current}, query);
  const multi = !((typeof options === 'object') ? (options.justOne) : false);
  return _this.updateRaw(modifiedQuery, {$set: {_current:CurrentStatus.Archived, _endDate: this.getCurrentDate()}}, {multi: multi})
  .then(function(result) {
    const { n, ok } = result;
    return { n, ok };
  });
};

TemporalCollection.prototype.find = function(query, projection) {
  const _this = this;
  const modifiedQuery = Object.assign({_current: CurrentStatus.Current}, query);
  return _this.findRaw(modifiedQuery, projection || {});
};

TemporalCollection.prototype.findOne = function(query, projection) {
  const _this = this;
  const modifiedQuery = Object.assign({_current: CurrentStatus.Current}, query);
  if(projection) {
    return _this.findOneRaw(modifiedQuery, projection);
  }
  else
  {
    return _this.findOneRaw(modifiedQuery);
  }
};

TemporalCollection.prototype.findOneByDate = function(query, date, projection) {
  const _this = this;
  const modifiedQuery = Object.assign({}, query);
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
  const _this = this;
  const modifiedQuery = Object.assign({}, query);
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
  const delay = this.config._retryMilliseconds * this.config._retryFactor * attempt;
  if(delay > this.config._retryMaxDelay) {
    delay = this.config._retryMaxDelay;
  }
  return delay;
};
// *****************************************************************************

// *****************************************************************************
// maintain a healthy temporal collection

TemporalCollection.prototype.temporalize = function() {
  const _this = this;

  const batchCount = 0;
  const documentsPerBatch = 50;

  return _this.count({_rId: {$exists: true}})
  .then(function(alreadyTemporalizedCount) {
    if(alreadyTemporalizedCount > 0) {
      return Promise.reject('Collection already temporalized.');
    } else {
      return _this.temporalizeBatch(pmongo.ObjectId('000000000000000000000000'), documentsPerBatch, batchCount);
    }
  })
  .catch(function(err) {
    return Promise.reject('Error Temporalizing: ' + JSON.stringify(err));
  });
};

TemporalCollection.prototype.temporalizeBatch = function(lastId, documentsPerBatch, batchCount) {
  batchCount++;
  const _this = this;
  return _this.findRaw({_rId: {$exists: false}, _id: {$gt: lastId}}, {_id: 1}).sort({_id: 1}).limit(documentsPerBatch).toArray()
  .then(function(docsToTemporalize) {
    if(docsToTemporalize && docsToTemporalize.length > 0) {
      const updatePromises = [];
      for(let docIter = 0; docIter < docsToTemporalize.length; docIter++) {
        const startDate = new Date(parseInt(docsToTemporalize[docIter]._id.toString().slice(0,8), 16)*1000);
        const docId = docsToTemporalize[docIter]._id;
        updatePromises.push(_this.findAndModifyRaw( { query: { _id:  docId}, update: { $set: { _current: CurrentStatus.Current, _rId: docId, _startDate: startDate, _endDate: _this.config._maxDate } }} ));
      }
      return Promise.all(updatePromises)
      .then(function() {
        return _this.temporalizeBatch(docsToTemporalize[docsToTemporalize.length-1]._id, documentsPerBatch, batchCount);
      });
    } else {
      return true;
    }
  });
};

TemporalCollection.prototype.ensureIndexes = function() {
  const _this = this;

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
  const _this = this;

  cleanBeforeDate = cleanBeforeDate || new Date();

  const cleaningPromises = [];

  const removeOrphanedCloneQuery = {
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

  const fixEndDateQuery = {
    _current: CurrentStatus.Current,
    _endDate: {
      $ne: _this.config._maxDate
    },
    _startDate: {
      $lt: cleanBeforeDate
    }
  };

  const fixEndDateUpdate = {
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

  const unlockCurrentDocumentsQuery = {
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

  const removeAllTranIdsQuery = {
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

  return Promise.all(cleaningPromises);
};

// *****************************************************************************

// *****************************************************************************
TemporalMongo.TemporalCollection = TemporalCollection;
TemporalMongo.CurrentStatus = CurrentStatus;
module.exports = TemporalMongo;
// *****************************************************************************
