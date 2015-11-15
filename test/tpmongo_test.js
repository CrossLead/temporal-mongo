var tpmongo = require('../');
var pmongo = require('promised-mongo');
var should = require('should');
var _ = require('lodash');
var massInsertCount = 1000;

/**
 * Pause for <ms>
 *
 * @param {Integer} milliseconds to wait
 * @return Promise<undefined> promise resolving after delay
 */
function delay(ms = 0) {
  return new Promise(res => setTimeout(res, ms));
}

/**
 * Test functions for map reduce
 */
function testMapping() {
  emit(this.c, this.b);
}

function testReduce(keyA, bValues) {
  return Array.sum(bValues);
}

describe('utility functions', () => {
  const { util } = tpmongo;

  it('tpmongo should include util functions', done => {
    should.exist(util);
    done();
  });

  it('util.omit should exclude properties', done => {
    should.deepEqual(util.omit({a:1, b:2}, 'b'), {a: 1});
    done();
  });

  it('util.addRawMethods should add all expected raw methods to prototype', done => {

    @util.addRawMethods([
      'find',
      'remove'
    ])
    class Test { }

    const t = new Test;

    should.exist(t.findRaw);
    should.exist(t.removeRaw);
    done();
  });

  it('util.sanitizeUpdate should exclude _current', done => {
    should.deepEqual(util.sanitizeUpdate({a:1, _current:1}), {a: 1});
    done();
  });

  it('util.addCurrent should add current status to query', done => {
    should.deepEqual(util.addCurrent({a: 1}), { a: 1, _current: tpmongo.CurrentStatus.Current });
    done();
  });

  it('util.addDate should add date restriction to query', done => {
    const date = new Date();
    should.deepEqual(
      util.addDate({a: 1}, date),
      { a: 1,
        _startDate: {$lte : date},
        _endDate:   {$gt  : date} }
    );
    done();
  });

});

describe('tpmongo', function() {
  this.timeout(30000);

  // drop test db for temporalization
  before(() => pmongo('localhost/tpmongoTestDb').dropDatabase());

  //setup
  var maxDate = new Date('2099-07-21 15:16:00.599Z');
  var mongoCollections = ['tempCollection'];

  var db = tpmongo('localhost/tpmongoTestDb', mongoCollections, { _maxDate: maxDate });

  var doc1Id = db.ObjectId('55addf649ce171641a34281d');
  var doc2Id = db.ObjectId('55addf649ce171641a34281e');
  var doc3Id = db.ObjectId('5609b87d282d4aac260fcb9f');

  var testStartDate = new Date('2015-07-21 15:16:00.599Z');
  var testEndDate = maxDate;
  var testMiddleDate = new Date('2015-07-22 15:16:00.599Z');

  var setupDocuments = function() {
    var doc1 = {a: 1, c:1, _current: 1, _id: doc1Id, _rId: doc1Id, _startDate: testStartDate, _endDate: testEndDate};
    var doc2 = {a: 1, b:2, c:2, _current: 1, _id: doc2Id, _rId: doc2Id, _startDate: testStartDate, _endDate: testEndDate};
    var doc3 = {a: 1, b:3, c:2, _current: 1, _id: doc3Id, _rId: doc3Id, _startDate: testStartDate, _endDate: testEndDate};

    return db.tempCollection.removeRaw({})
    .then(function() {
      return db.tempCollection.insertRaw(doc1);
    })
    .then(function() {
      return db.tempCollection.insertRaw(doc2);
    })
    .then(function() {
      return db.tempCollection.insertRaw(doc3);
    })
    .then(function() {
      return delay(50);
    });
  };

  var massInsertTest = function() {
    var docs = [];
    for(var i = 0; i < massInsertCount; i++) {
      docs.push({a:1});
    }
    return db.tempCollection.removeRaw({})
    .then(function() {
      return db.tempCollection.insertRaw(docs);
    });
  };

  it('should maintain API consistent w/promised-mongo', function() {
    should(db.collection(mongoCollections[0]) instanceof tpmongo.TemporalCollection).equal(true);
    should.exist(tpmongo.ObjectId);
  });

  it('temporalize works', function () {
    var actionWorked = false;
    return massInsertTest()
    .then(function() {
      return db.tempCollection.temporalize();
    })
    .then(function() {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('retemporalize should not work', function () {
    var actionWorked = false;
    return massInsertTest()
    .then(function() {
      return db.tempCollection.temporalize();
    })
    .then(function() {
      return db.tempCollection.temporalize();
    })
    .then(function() {
      actionWorked = true;
    })
    .catch(function() {
      //this should actually happen for this test
    })
    .then(function() {
      actionWorked.should.equal(false);
    });
  });

  it('ensureIndexes works', function () {
    var actionWorked = false;
    return massInsertTest()
    .then(function() {
      return db.tempCollection.ensureIndexes();
    })
    .then(function() {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('cleanLocks should kill all tran fields', function () {
    var badDocCount = 1;
    return massInsertTest()
    .then(function() {
      return db.tempCollection.cleanLocks();
    })
    .then(function() {
      return db.tempCollection.countRaw({$or: [{_tranIds: {$exists: true}}, {_current: {$eq: 2}}, {_locked: {$exists: true}}]});
    })
    .then(function(result) {
      badDocCount = result;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      badDocCount.should.be.exactly(0);
    });
  });

  it('tpmongo should be object', function () {
    (typeof db).should.equal('object');
  });

  it('mass insertRaw works', function () {
    var docCount = 0;
    return massInsertTest()
    .then(function() {
      return db.tempCollection.countRaw({});
    })
    .then(function(theCount) {
      docCount = theCount;
    })
    .then(function() {
      docCount.should.be.exactly(massInsertCount);
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
      throw err;
    });
  });

  it('count after update should work', function () {
    var docCount = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {c: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.count({});
    })
    .then(function(theCount) {
      docCount = theCount;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      docCount.should.be.exactly(3);
    });
  });

  it('count by date should work', function () {
    var docCount = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {c: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.countByDate({}, testMiddleDate);
    })
    .then(function(theCount) {
      docCount = theCount;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      docCount.should.be.exactly(3);
    });
  });

  it('distinct after update should work', function () {
    var distinctValueCount = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {c: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.distinct('b', {a: 1});
    })
    .then(function(distinctValues) {
      distinctValueCount = distinctValues.length;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      distinctValueCount.should.be.exactly(2);
    });
  });

  it('distinct by date should work', function () {
    var distinctValueCount = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.distinctByDate('b', {a: 1}, testMiddleDate);
    })
    .then(function(distinctValues) {
      distinctValueCount = distinctValues.length;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      distinctValueCount.should.be.exactly(2);
    });
  });



  it('mapReduce should work', function () {
    var mapReduceReturnValue = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.mapReduce(testMapping, testReduce, {out: {inline: 1}, query: {a:1}});
    })
    .then(function(results) {
      var result = results.results;
      for(var resultIter = 0; resultIter < result.length; resultIter++) {
        mapReduceReturnValue += result[resultIter].value;
      }
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      mapReduceReturnValue.should.be.exactly(12);
    });
  });

  it('mapReduce by date should work', function () {
    var mapReduceReturnValue = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.mapReduceByDate(testMapping, testReduce, {out: {inline: 1}, query: {a:1}}, testMiddleDate);
    })
    .then(function(results) {
      var result = results.results;
      for(var resultIter = 0; resultIter < result.length; resultIter++) {
        mapReduceReturnValue += result[resultIter].value || 0;
      }
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      mapReduceReturnValue.should.be.exactly(5);
    });
  });

  it('aggregate should work', function () {
    var aggregateTotal = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.aggregate([{$group: {_id: '$a', total: {$sum: '$b'}}}]);
    })
    .then(function(result) {
      for(var resultIter = 0; resultIter < result.length; resultIter++) {
        aggregateTotal += result[resultIter].total;
      }
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      aggregateTotal.should.be.exactly(12);
    });
  });

  it('aggregate by date should work', function () {
    var aggregateTotal = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.aggregateByDate(testMiddleDate, [{$group: {_id: '$a', total: {$sum: '$b'}}}]);
    })
    .then(function(result) {
      for(var resultIter = 0; resultIter < result.length; resultIter++) {
        aggregateTotal += result[resultIter].total;
      }
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      aggregateTotal.should.be.exactly(5);
    });
  });

  it('api consistency - find', function () {
    var tpMongoResult = {};
    var pMongoResult = {};

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.find({a: 1}).toArray();
    })
    .then(function(result) {
      tpMongoResult = result;
      return db.tempCollection.findRaw({_current: 1, a: 1}).toArray();
    })
    .then(function(result) {
      pMongoResult = result;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      should.deepEqual(tpMongoResult, pMongoResult);
    });
  });

  it('api consistency - findOne', function () {
    var tpMongoResult = {};
    var pMongoResult = {};

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.findOne({b: 3});
    })
    .then(function(result) {
      tpMongoResult = result;
      return db.tempCollection.findOneRaw({_current: 1, b: 3});
    })
    .then(function(result) {
      pMongoResult = result;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      should.deepEqual(tpMongoResult, pMongoResult);
    });
  });

  it('api consistency - findAndModify', function () {
    var tpMongoResult = {};
    var pMongoResult = {};

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findAndModify({query: {b: 3}, update: {$set: {anotherProperty: 2}}});
    })
    .then(function(result) {
      tpMongoResult = result;
      tpMongoResult.value = _.pick(tpMongoResult.value, ['a','b','c']);
      tpMongoResult.lastErrorObject.connectionId = 0; // allow for different connection ids
      tpMongoResult = _.pick(tpMongoResult, ['lastErrorObject','ok','value']);
      return db.tempCollection.findAndModifyRaw({query: {_current: 1, b: 3}, update: {$set: {anotherProperty: 2}}});
    })
    .then(function(result) {
      pMongoResult = result;
      pMongoResult.value = _.pick(pMongoResult.value, ['a','b','c']);
      pMongoResult.lastErrorObject.connectionId = 0; // allow for different connection ids
      pMongoResult = _.pick(pMongoResult, ['lastErrorObject','ok','value']);
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      should.deepEqual(tpMongoResult, pMongoResult);
    });
  });

  it('api consistency - update', function () {
    var tpMongoResult = {};
    var pMongoResult = {};

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function(result) {
      tpMongoResult = result;
      return setupDocuments();
    })
    .then(function() {
      return db.tempCollection.updateRaw({_current: 1, a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function(result) {
      pMongoResult = result;
      // need to double the number modified to match tpmongo (which also must update the current doc)
      pMongoResult.nModified *= 2;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      should.deepEqual(tpMongoResult, pMongoResult);
    });
  });

  it('api consistency - remove', function () {
    var tpMongoResult = {};
    var pMongoResult = {};

    return setupDocuments()
    .then(function() {
      return db.tempCollection.remove({a: 1});
    })
    .then(function(result) {
      tpMongoResult = result;
      return setupDocuments();
    })
    .then(function() {
      return db.tempCollection.removeRaw({_current: 1, a: 1});
    })
    .then(function(result) {
      pMongoResult = result;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      should.deepEqual(tpMongoResult, pMongoResult);
    });
  });

  it('initialize works', function () {
    var docCount = 0;
    var initializedCount = 0;
    // var initializationComplete = false;
    return massInsertTest()
    .then(function() {
      return db.tempCollection.countRaw({});
    })
    .then(function(theCount) {
      docCount = theCount;
    })
    .then(function() {
      return db.tempCollection.countRaw({_rId: {$exists: true}});
    })
    .then(function(alreadyInitializedCount) {
      initializedCount = alreadyInitializedCount;

      if(initializedCount < 0) {

      }
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      docCount.should.be.exactly(massInsertCount);
    });
  });

  it('count should be 3 after inserts', function () {
    var docCount = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.count({});
    })
    .then(function(theCount) {
      docCount = theCount;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      docCount.should.be.exactly(3);
    });
  });

  it('aggregate works', function () {
    var docCount = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.count({});
    })
    .then(function(theCount) {
      docCount = theCount;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      docCount.should.be.exactly(3);
    });
  });

  it('find count should be 3', function () {
    var docCount = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.find({a: 1}).toArray()
    })
    .then(function(foundDocuments) {
      if(foundDocuments) {
        docCount = foundDocuments.length;
      }
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      docCount.should.be.exactly(3);
    });
  });

  it('find by date should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findByDate({a: 1}, new Date())
    })
    .then(function(/* findResult */) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('find one should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findOne({a: 1})
    })
    .then(function(findOneResult) {
      if(findOneResult && findOneResult.a === 1) {
        actionWorked = true;
      }
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('find one by date should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findOneByDate({a: 1}, new Date())
    })
    .then(function(/* findOneResult */) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });


  it('findAndModify (std) should work', function () {
    return setupDocuments()
      .then(function() {
        return db.tempCollection.findAndModify({
          query: {_rId: doc1Id},
          update: {$set: {anotherProperty: 2}},
          new: true
        });
      })
      .then(function(result) {
        result.value.anotherProperty.should.equal(2);
      })
      .catch(function(err) {
        console.log('Error: ');
        console.log(err);
      });
  });

  it('findAndModify (std) replacing entire doc should work', function () {
    return setupDocuments()
      .then(function() {
        return db.tempCollection.findAndModify({
          query: {_rId: doc1Id},
          update: { onlyProp: 'onlyVal' },
          new: true
        });
      })
      .then(function(result) {
        doc1Id.equals(result.value._rId).should.equal(true);
        result.value.onlyProp.should.equal('onlyVal');
        should.not.exist(result.value.a);
      })
      .catch(function(err) {
        console.log('Error: ');
        console.log(err);
      });
  });

  it('findAndModify (upsert) should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findAndModify({query: {x: 1}, upsert: true, update: {$set: {x: 1, anotherProperty: 2}}});
    })
    .then(function(/* foundDocuments */) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('update (std) should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {c: 4}}, {multi: true});
    })
    .then(function(/* updateResult */) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('update (upsert) should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {c: 4}}, {multi: true, upsert: true});
    })
    .then(function(/* updateResult */) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('save with _rId should work', function () {
    return setupDocuments()
      .then(function() {
        return db.tempCollection.save({_rId: doc3Id, d: 4});
      })
      .then(function(saveResult) {
        should.exist(saveResult._rId);
        should(saveResult._rId.equals(doc3Id)).equal(true);
        should.not.exist(saveResult.a);
        saveResult.d.should.equal(4);
      });
  });

  it('save with unknown _rId should work', function () {
    return setupDocuments()
      .then(function() {
        return db.tempCollection.save({_rId: db.ObjectId(), d: 5});
      })
      .then(function(saveResult) {
        should.exist(saveResult._rId);
        saveResult.d.should.equal(5);
      });
  });

  it('save with NO _rId should work', function () {
    return setupDocuments()
      .then(function() {
        return db.tempCollection.save({d: 6});
      })
      .then(function(saveResult) {
        should.exist(saveResult._rId);
        saveResult.d.should.equal(6);
      });
  });

  it('remove should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.remove({a: 1});
    })
    .then(function(/* saveResult */) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });

  it('perf test should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      //var returnObject = {success: true, errors: []};

      var tempValue = new Date();
      //var query = {'tempProperty': tempValue};
      var queryMultiple = {'sameValue': 1};
      var update = {$set: {'anotherProperty': 1}};
      var numberOfUpdatesToTest = 5;

      var insertPromises = [];
      for(var j = 0; j < 100; j++)
      {
        insertPromises.push(db.tempCollection.insert({'tempProperty': tempValue, 'sameValue': 1}, {writeConcern: 'majority'}));
        insertPromises.push(db.tempCollection.insert({'tempProperty': tempValue, 'sameValue': 3, doc2val:2}, {writeConcern: 'majority'}));
      }

      return Promise.all(insertPromises)
      .then(function() {
        var updatePromises = [];
        for(var i = 0; i < numberOfUpdatesToTest; i++) {
          updatePromises.push(db.tempCollection.update(queryMultiple, update, {multi: true}));
          updatePromises.push(db.tempCollection.findAndModify({query: {a: 1}, update: {$set: {'anotherProperty': 2}}}));
        }

        return Promise.all(updatePromises);
      });
    })
    .then(function(/* saveResult */) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .then(function() {
      actionWorked.should.equal(true);
    });
  });
});
