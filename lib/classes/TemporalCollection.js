import pmongo                   from 'promised-mongo';
import PromisedMongoCollection  from 'promised-mongo/dist/Collection';
import CurrentStatus            from '../enum/CurrentStatus';

import {  omit,
          sanitizeUpdate,
          delay,
          addDate,
          addCurrent,
          addRawMethods  }      from '../util/';


/**
 * Decorator to add raw promised mongo methods ( col.findRaw(...) )
 */
@addRawMethods([
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
])
export default class TemporalCollection extends PromisedMongoCollection {

  constructor(db, name, config) {
    super(db, name);
    this.setConfig(config);

    // store pmongo collection in proxy
    db.proxyCollections[name] = db[name];

    // set main collection to this
    db[name] = this;

    // set proxy for this collection
    db[name].proxy = db.proxyCollections[name];
  }

  // config and error handling
  setConfig(config) {
    // take from the given settings or use default settings
    this.config = {
      _retryAttempts:       20,
      _retryMilliseconds:   15,
      _retryFactor:         5,
      _retryMaxDelay:       10000,
      _maxDate:             new Date('2099-01-01 00:00:00.000Z'),
      ...config
    };
  }

  temporalMongoError(errorCode, errorMessage) {
    this.info('Throwing Error: ' + errorMessage);
    return {
      errorCode,
      errorMessage,
      success: false
    };
  }

  /**
   * For a `TemporalCollection`, `save()` semantics differ as follows:
   *  * Insertion vs update depends on `_rId` field vs `_id`
   * @param  {Object} doc document to save
   * @param  {Object} options primarily `writeConcern`
   * @return {Object} inserted or updated document
   */
  async save(doc, options) {
    const { _rId } = doc,
          insertDoc = omit(doc, ['_id', '_rId']);

    if(_rId === undefined) {
      return this.insert(insertDoc, options);
    }

    const result = await this.findAndModify({
      query:  { _rId },
      update: { $set: insertDoc },
      new:    true,
      upsert: true
    });

    // Match promised-mongo save() format, which is just the doc
    return result.value;
  }


  /**
   * Add restrictions to aggregation pipeline for temporal docs
   *
   * @param {Function} restriction : function to modify initial $match
   * @param {Date} date : date for temporal restriction
   * @param {Array} pipeline : aggregation pipeline
   * @param {Object} options : aggregation options
   * @return {Promise}
   */
  _aggregateWrapper(restriction, date, pipeline) {
    if (!Array.isArray(pipeline)) {
      throw new Error(`Temporal Mongo: pipeline must be an array!`);
    }

    if (!pipeline.length) {
      return this.aggregateRaw();
    }

    // first element of pipeline should be $match to current
    const [ first ] = pipeline;

    if (first.$match) {
      first.$match = restriction(first.$match, date);
    } else {
      pipeline.unshift({ $match: restriction({}, date) });
    }

    return this.aggregateRaw(...pipeline);
  }

  aggregate(pipeline) {
    return this._aggregateWrapper(addCurrent, null, pipeline);
  }

  aggregateByDate(date, pipeline) {
    if (!(date && date instanceof Date)) {
      throw new Error('date required for aggregateByDate, none given!');
    }
    return this._aggregateWrapper(addDate, date, pipeline);
  }

  count(query) {
    return this.countRaw(addCurrent(query));
  }

  countByDate(query, date) {
    return this.countRaw(addDate(query, date));
  }

  distinct(field, query) {
    return this.distinctRaw(field, addCurrent(query));
  }

  distinctByDate(field, query, date) {
    return this.distinctRaw(field, addDate(query, date));
  }

  mapReduce(map, reduce, options={}) {
    return this.mapReduceRaw(map, reduce, { ...options, query: addCurrent(options.query) });
  }

  mapReduceByDate(map, reduce, options={}, date) {
    return this.mapReduceRaw(map, reduce, { ...options, query: addDate(options.query, date) });
  }

  insert(doc, options) {
    return this.insertRaw({
      ...doc,
      _rId:       doc._rId || pmongo.ObjectId(),
      _id:        doc._rId,
      _current:   CurrentStatus.Current,
      _startDate: this.getCurrentDate(),
      _endDate:   this.config._maxDate
    }, options);
  }

  makeClone(doc, currentDate, tranId) {
    return {
      ...omit(doc, '_transId'),
      _id:          pmongo.ObjectId(),
      _current:     CurrentStatus.InProgressClone,
      _startDate:   currentDate,
      _endDate:     this.config._maxDate,
      _cloneTranId: tranId,
    };
  }

  insertClone(doc, currentDate, tranId) {
    return this.insertRaw(
      omit(this.makeClone(doc, currentDate, tranId), '_endDate'),
      {writeConcern: 'majority'}
    );
  }

  async update(query, docUpdate, options, transactionId, attempt=0) {
    attempt++;

    const currentDate                   = this.getCurrentDate(),
          tranId                        = transactionId || pmongo.ObjectId(),
          queryForCurrent               = { _current: CurrentStatus.Current, ...query },
          queryForCurrentByTransaction  = { '_tranIds.0': tranId, ...queryForCurrent},
          optionsForLocking             = { writeConcern: 'majority', ...options},
          sanitizedDocUpdate            = sanitizeUpdate(docUpdate),
          sendErr                       = (...args) => Promise.reject(this.temporalMongoError(...args));

    let lockedDocumentCount = 0;

    this.info('Starting Phase 1');

    const updateResult = await this.updateRaw(
      queryForCurrent,
      { $push: {_tranIds: tranId},
        $set:  {_endDate: currentDate} },
      optionsForLocking
    );

    lockedDocumentCount = updateResult.n;

    const sourceDocuments = await this.findRaw(queryForCurrentByTransaction).toArray();

    if(lockedDocumentCount === 0) {
      if(options && options.upsert === true) {
        const upsertId = pmongo.ObjectId();
        await this.insert({_id: upsertId});
        return this.updateRaw({_id: upsertId}, sanitizedDocUpdate);
      } else {
        return true;
      }
    }

    //if the find pulls a different number of records, then two updates were attempted at the same time
    if(sourceDocuments.length !== lockedDocumentCount) {
      await this.updateRaw({'_tranIds': tranId}, {$pull: { _tranIds: tranId }}, optionsForLocking)
      try {
        if(attempt > this.config._retryAttempts) {
          return sendErr('DEADLOCK_DETECTED', 'Two concurrent transactions were detected.');
        } else {
          const nextDelay = this.getNextDelay(attempt);
          await delay(nextDelay)
          this.info('Attempt: ' + attempt + ', ' + nextDelay);
          return this.update(query, sanitizedDocUpdate, options, tranId, attempt);
        }
      } catch (err) {
        return sendErr(
            'DEADLOCK_DETECTED_LOCK_NOT_RELEASED',
            'Two concurrent transactions were detected, and the lock could not be released: ' + JSON.stringify(err)
          );
      }
    } else {
      this.info('Starting Phase 2');
      // PHASE 2, insert clones - rId propogates
      const clones = sourceDocuments.map(doc => this.makeClone(doc, currentDate, tranId));

      await this.insertRaw(clones, {writeConcern: 'majority'});

      try {
        this.info('Starting Phase 3');
        // PHASE 3, update clone
        this.info(JSON.stringify(sanitizedDocUpdate));
        await this.updateRaw({_cloneTranId: tranId}, sanitizedDocUpdate, {multi: true, writeConcern: 'majority'});
      } catch (cloneInsertError) {
        try {
          //rollback locked docs
          console.log('tpmongo cloneInsertError - check unique indexes');
          console.log(cloneInsertError);

          await this.updateRaw(
            { _tranIds: tranId },
            { $pull: { _tranIds: tranId },
              $set: { _endDate: this.config._maxDate } },
            { multi: true, writeConcern: 'majority' }
          );

          return sendErr('CLONE_INSERTS_FAILED', 'The clone insert(s) failed: ');
        } catch (err) {
          return sendErr(
            'CLONE_INSERTS_FAILED_LOCK_NOT_RELEASED',
            'The clone insert(s) failed - lock not release: ' + JSON.stringify(err)
          );
        }
      }

      try {
        this.info('Starting Phase 4');
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

        try {
          const updateResult = await this.updateRaw(
            unlockingQuery,
            unlockingUpdate,
            {multi: true, writeConcern: 'majority'}
          );

          if(updateResult.n !== 2*lockedDocumentCount) {
            return sendErr(
              'LOCK_RELEASE_FAILED',
              'Incorrect number of clones and current documents updated. Expected: ' +
                2*lockedDocumentCount +
              '. Got: ' +
                updateResult.n
            );
          }

          // Remap the updated doc count since we updated all clones and currents in 1 step
          updateResult.n = lockedDocumentCount;
          return updateResult;
        } catch (err) {
          return sendErr('LOCK_RELEASE_FAILED', 'The lock release failed: ' + JSON.stringify(err));
        }

      } catch (err) {
        return sendErr('PROCESS_FAILED', 'The update process failed: ' + JSON.stringify(err));
      }
    }

  }

  async findAndModify(options, attempt=0) {

    const currentDate  = this.getCurrentDate(),
          currentQuery = { _current: CurrentStatus.Current, ...options.query },
          docUpdate    = sanitizeUpdate(options.update),
          sort         = options.sort || {},
          sendErr      = (...args) => Promise.reject(this.temporalMongoError(...args));

    if (options && options.remove === true) {
      return sendErr('UNSUPPORTED_OPERATION', 'Remove is not supported on findAndModify.');
    }

    const result = await this.findAndModifyRaw({
      query:  currentQuery,
      sort:   sort,
      update: { $set: {_locked: 1, _endDate: currentDate} }
    });

    try {
      if(result.value && result.value._locked === undefined) {

        const clone      = result.value,
              originalId = clone._id;

        Object.assign(clone, {
          _id:        pmongo.ObjectId(),
          _current:   CurrentStatus.InProgressClone,
          _startDate: currentDate,
          _endDate:   this.config._maxDate
        });

        try {
          await this.insertRaw(clone)
          this.addUnsetterToUpdate(docUpdate, '_locked');
          await this.updateRaw({_id: clone._id}, docUpdate);
        } catch (err) {
          return sendErr('CLONE_INSERT_FAILED', 'The clone insert failed: ' + JSON.stringify(err));
        }

        try {
          //phase 4: simultaneously do following:
          // * unlock and archive original
          // * set clone as current
          const unlockQuery   = {_id: {$in: [originalId, clone._id]}},
                unlockUpdate  = {$inc: {_current: -1}, $unset: {_locked: ''}},
                unlockOpts    = {multi: true, writeConcern: 'majority'};

          try {
            await this.updateRaw(unlockQuery, unlockUpdate, unlockOpts);
            //mimic old api return type
            if(options.new) {
              return this.findAndModifyRaw({query: {_id: clone._id}, update: {$unset: {fieldThatDoesNotExist:''}}});
            } else {
              return this.findAndModifyRaw({query: {_id: originalId}, update: {$unset: {fieldThatDoesNotExist:''}}});
            }
          } catch (err) {
            return sendErr('LOCK_RELEASE_FAILED', 'The lock release failed: ' + JSON.stringify(err));
          }

        } catch (err) {
          return sendErr('PROCESS_FAILED', 'The clone/update process failed: ' + JSON.stringify(err));
        }
      }
      else if(result.value && result.value._locked !== undefined) {
        attempt++;
        const { _retryAttempts: tries } = this.config;
        if(attempt > tries) {
          return sendErr('EXCEEDED_RETRY_ATTEMPTS', `Document locked. ${tries} attempts exceeded.`);
        }
        else {
          await delay(this.getNextDelay(attempt))
          return this.findAndModify(options, attempt);
        }
      }
      else if (options.upsert) {
        const upsertId =  currentQuery._rId                                       ||
                          (docUpdate.$setOnInsert && docUpdate.$setOnInsert._rId) ||
                          (docUpdate.$set && docUpdate.$set._rId)                 ||
                          pmongo.ObjectId();

        await this.insert({_rId: upsertId});
        await this.updateRaw({_rId: upsertId}, docUpdate);

        // Match findAndModify() result format
        if(options.new) {
          return this.findAndModifyRaw({query: {_rId: upsertId}, update: {$unset: {fieldThatDoesNotExist:''}}});
        } else {
          return null;
        }
      }
      else {
        return sendErr('NO_RESULTS', 'findAndModify returned no results.');
      }
    } catch (err) {
      return sendErr('FINDANDMODIFY_ERROR', 'findAndModify returned an error: ' + JSON.stringify(err));
    }

  }

  async remove(query, options) {
    const modifiedQuery = { _current: CurrentStatus.Current, ...query },
          multi         = !((typeof options === 'object') ? (options.justOne) : false);

    const { n, ok } = await this.updateRaw(
      modifiedQuery,
      { $set: {_current:CurrentStatus.Archived, _endDate: this.getCurrentDate()} },
      { multi: multi }
    )

    return { n, ok };
  }

  find(query, projection={}) {
    return this.findRaw(addCurrent(query), projection);
  }

  findOne(query, projection) {
    return this.findOneRaw(addCurrent(query), projection);
  }

  findOneByDate(query, date, projection) {
    return this.findOneRaw(addDate(query, date), projection);
  }

  findByDate(query, date, projection) {
    return this.findRaw(addDate(query, date), projection);
  }

  addUnsetterToUpdate(update, propertyToUnset) {
    update.$unset = update.$unset || {};
    update.$unset[propertyToUnset] = '';
  }

  addSetterToUpdate(update, propertyToSet, value) {
    update.$set = update.$set || {};
    update.$set[propertyToSet] = value;
  }

  getCurrentDate() {
    return new Date();
  }

  info(/* logText */) {
    //console.log(logText);
  }

  getNextDelay(attempt) {
    const { _retryFactor, _retryMaxDelay, _retryMilliseconds } = this.config;
    let delay = _retryMilliseconds * _retryFactor * attempt;
    if(delay > _retryMaxDelay) {
      delay = _retryMaxDelay;
    }
    return delay;
  }

  async temporalize() {
    const batchCount = 0,
          documentsPerBatch = 50;

    try {
      const alreadyTemporalizedCount = await this.count({_rId: {$exists: true}});

      if(alreadyTemporalizedCount > 0) {
        return Promise.reject('Collection already temporalized.');
      } else {
        return this.temporalizeBatch(pmongo.ObjectId('000000000000000000000000'), documentsPerBatch, batchCount);
      }
    } catch (err) {
      return Promise.reject('Error Temporalizing: ' + JSON.stringify(err));
    }
  }

  async temporalizeBatch(lastId, documentsPerBatch, batchCount) {
    batchCount++;

    const docsToTemporalize = await this
      .findRaw({_rId: {$exists: false}, _id: {$gt: lastId}}, {_id: 1})
      .sort({_id: 1})
      .limit(documentsPerBatch)
      .toArray();

    if(docsToTemporalize && docsToTemporalize.length > 0) {

      await* docsToTemporalize.map(doc => {
        const startDate = new Date(parseInt(doc._id.toString().slice(0,8), 16)*1000),
              docId = doc._id;

        return this.findAndModifyRaw({
          query:  { _id:  docId },
          update: {
            $set: {
              _current:   CurrentStatus.Current,
              _rId:       docId,
              _startDate: startDate,
              _endDate:   this.config._maxDate
            }
          }
        });
      });

      return this.temporalizeBatch(
        docsToTemporalize[docsToTemporalize.length-1]._id,
        documentsPerBatch,
        batchCount
      );
    } else {
      return true;
    }
  }

  async ensureIndexes() {
    return await* [
      this.ensureIndex({_current: CurrentStatus.Current}),
      this.ensureIndex({_transIds: 1}),
      this.ensureIndex({_current: CurrentStatus.Current, _transIds: 1}),
      this.ensureIndex({_rId: 1}),
      // Must use runCommand until Mongo 3.1.8 releases (w/updated driver)
      this.runCommand('createIndexes', {
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
  }

  async cleanLocks(cleanBeforeDate=new Date()) {

    const removeOrphanedCloneQuery = {
      _current: CurrentStatus.InProgressClone,
      _startDate: {
        $lt: cleanBeforeDate
      }
    };

    const fixEndDateQuery = {
      _current: CurrentStatus.Current,
      _endDate: {
        $ne: this.config._maxDate
      },
      _startDate: {
        $lt: cleanBeforeDate
      }
    };

    const fixEndDateUpdate = {
      $set: {
        _endDate: this.config._maxDate
      }
    };

    const unlockCurrentDocumentsQuery = {
      _current: CurrentStatus.Current,
      _locked: 1,
      _startDate: {
        $lt: cleanBeforeDate
      }
    };

    const removeAllTranIdsQuery = {
      _tranIds: {
        $exists: true
      },
      _startDate: {
        $lt: cleanBeforeDate
      }
    };

    return await* [
      this.removeRaw(removeOrphanedCloneQuery, {writeConcern: 'majority'})
          .then(() => {
            this.info('Removed Orphaned Clones');
            return true;
          }),

      this.updateRaw(fixEndDateQuery, fixEndDateUpdate, {multi: true, writeConcern: 'majority'})
          .then(() => {
            this.info('Fixed End Dates');
            return true;
          }),

      this.updateRaw(
            unlockCurrentDocumentsQuery,
            { $unset: {_locked: ''},
              $set: {_endDate: this.config._maxDate} },
            { multi: true, writeConcern: 'majority' }
          )
          .then(() => {
            this.info('Unset locked');
            return true;
          }),

      this.updateRaw(removeAllTranIdsQuery, {$unset: {_tranIds: ''}}, {multi: true, writeConcern: 'majority'})
        .then(() => {
          this.info('Removed all tranIds');
          return true;
        })
    ];
  }

}
