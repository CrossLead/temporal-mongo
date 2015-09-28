var tpmongo = require('../');
var should = require('should');
var Q = require('q');
var _ = require('lodash');
var massInsertCount = 1000;

describe('tpmongo', function () {

  //setup 
  var maxDate = new Date('2099-07-21 15:16:00.599Z');
  var mongoCollections = ['tempCollection'];
  var db = tpmongo('localhost/mongoTestDb', mongoCollections, { _maxDate: maxDate });

  var testObjectId = db.ObjectId('5609b87d282d4aac260fcb9f');
  var testStartDate = new Date('2015-07-21 15:16:00.599Z');
  var testEndDate = maxDate;
  var testMiddleDate = new Date('2015-07-22 15:16:00.599Z');

  var setupDocuments = function() {
    var doc1 = {a: 1, c:1, _current: 1, _id: db.ObjectId('55addf649ce171641a34281d'), _rId: db.ObjectId('55addf649ce171641a34281d'), _startDate: testStartDate, _endDate: testEndDate};
    var doc2 = {a: 1, b:2, c:2, _current: 1, _id: db.ObjectId('55addf649ce171641a34281e'), _rId: db.ObjectId('55addf649ce171641a34281e'), _startDate: testStartDate, _endDate: testEndDate};
    var doc3 = {a: 1, b:3, c:2, _current: 1, _id: db.ObjectId('5609b87d282d4aac260fcb9f'), _rId: db.ObjectId('5609b87d282d4aac260fcb9f'), _startDate: testStartDate, _endDate: testEndDate};

    return db.tempCollection.removeRaw({}, {w: 'majority'})
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
      return Q.delay(50);
    });
  };

  var massInsertTest = function() {  
    var docs = [];
    for(var i = 0; i < massInsertCount; i++) {
      docs.push({a:1});
    }
    return db.tempCollection.removeRaw({}, {w: 'majority'})
    .then(function() {
      return db.tempCollection.insertRaw(docs, {w: 'majority'});
    });
  };

  it('should maintain API consistent w/promised-mongo', function() {
    should(db.collection(mongoCollections[0]) instanceof tpmongo.TemporalCollection).equal(true);
    should.exist(tpmongo.ObjectId);
  });

  it('temporlize works', function () {
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
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });

  it('retemporlize should not work', function () {
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
    .finally(function() {
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
    .finally(function() {
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
    .finally(function() {
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
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      docCount.should.be.exactly(massInsertCount);
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
    .finally(function() {
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
    .finally(function() {
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
    .finally(function() {
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
    .finally(function() {
      distinctValueCount.should.be.exactly(2);
    });
  });

  var testMapping = function() {
    emit(this.c, this.b);
  };

  var testReduce = function(keyA, bValues) {
    return Array.sum(bValues);
  };

  it('mapReduce should work', function () {
    var mapReduceReturnValue = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function() {
      return db.tempCollection.mapReduce(testMapping, testReduce, {out: {inline: 1}, query: {a:1}});
    })
    .then(function(result) {
      for(var resultIter = 0; resultIter < result.length; resultIter++) {
        mapReduceReturnValue += result[resultIter].value;
      }
    })  
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      mapReduceReturnValue.should.be.exactly(12);
    });
  });

  it('mapReduce by date should work', function () {
    var mapReduceReturnValue = 0;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.mapReduceByDate(testMapping, testReduce, {out: {inline: 1}, query: {a:1}}, testMiddleDate);
    })
    .then(function(result) {
      for(var resultIter = 0; resultIter < result.length; resultIter++) {
        mapReduceReturnValue += result[resultIter].value || 0;
      }
    })  
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
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
      return db.tempCollection.aggregate({}, {$group: {_id: '$a', total: {$sum: '$b'}}});
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
    .finally(function() {
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
      return db.tempCollection.aggregateByDate({}, {$group: {_id: '$a', total: {$sum: '$b'}}}, testMiddleDate);
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
    .finally(function() {
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
    .finally(function() {
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
    .finally(function() {
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
      tpMongoResult[0] = _.pick(tpMongoResult[0], ['a','b','c']);
      tpMongoResult[1].value = _.pick(tpMongoResult[1].value, ['a','b','c']);
      tpMongoResult[1].lastErrorObject.connectionId = 0; // allow for different connection ids
      tpMongoResult[1] = _.pick(tpMongoResult[1], ['lastErrorObject','ok','value']);
      return db.tempCollection.findAndModifyRaw({query: {_current: 1, b: 3}, update: {$set: {anotherProperty: 2}}});
    })
    .then(function(result) {
      pMongoResult = result;
      pMongoResult[0] = _.pick(pMongoResult[0], ['a','b','c']);
      pMongoResult[1].value = _.pick(pMongoResult[1].value, ['a','b','c']);
      pMongoResult[1].lastErrorObject.connectionId = 0; // allow for different connection ids
      pMongoResult[1] = _.pick(pMongoResult[1], ['lastErrorObject','ok','value']);
    })  
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
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
      
      return db.tempCollection.updateRaw({_current: 1, a: 1}, {$set: {b: 4}}, {multi: true});
    })
    .then(function(result) {
      pMongoResult = result;
    })  
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
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
    .finally(function() {
      should.deepEqual(tpMongoResult, pMongoResult);
    });
  });

  it('initialize works', function () {
    var docCount = 0;
    var initializedCount = 0;
    var initializationComplete = false;
    return massInsertTest()
    .then(function() {
      return db.tempCollection.countRaw({});
    })
    .then(function(theCount) {
      docCount = theCount;
    })
    .then(function(theCount) {
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
    .finally(function() {
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
    .finally(function() {
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
    .finally(function() {
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
    .finally(function() {
      docCount.should.be.exactly(3);
    });
  });

  it('find by date should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findByDate({a: 1}, new Date())
    })
    .then(function(findResult) {
      actionWorked = true;  
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
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
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });

  it('find one by date should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findOneByDate({a: 1}, new Date())
    })
    .then(function(findOneResult) {
      actionWorked = true;  
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });


  it('findAndModify (std) should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findAndModify({query: {a: 1}, update: {$set: {anotherProperty: 2}}});
    })
    .then(function(foundDocuments) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });

  it('findAndModify (upsert) should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.findAndModify({query: {x: 1}, upsert: true, update: {$set: {x: 1, anotherProperty: 2}}});
    })
    .then(function(foundDocuments) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });

  it('update (std) should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {c: 4}}, {multi: true});
    })
    .then(function(updateResult) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });

  it('update (upsert) should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.update({a: 1}, {$set: {c: 4}}, {multi: true, upsert: true});
    })
    .then(function(updateResult) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });

  it('save with _rId should work', function () {
    return setupDocuments()
      .then(function() {
        return db.tempCollection.save({_rId: testObjectId, d: 4});
      })
      .then(function(saveResult) {
        should.exist(saveResult._rId);
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
    .then(function(saveResult) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });

  it('perf test should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      var returnObject = {success: true, errors: []};

      var tempValue = new Date();
      var query = {'tempProperty': tempValue};
      var queryMultiple = {'sameValue': 1};
      var update = {$set: {'anotherProperty': 1}};
      var numberOfUpdatesToTest = 5;

      var insertPromises = [];
      for(var j = 0; j < 100; j++)
      {
        insertPromises.push(db.tempCollection.insert({'tempProperty': tempValue, 'sameValue': 1}, {writeConcern: 'majority'}));
        insertPromises.push(db.tempCollection.insert({'tempProperty': tempValue, 'sameValue': 3, doc2val:2}, {writeConcern: 'majority'}));
      }

      return Q.all(insertPromises)
      .then(function() {
        var updatePromises = [];
        for(var i = 0; i < numberOfUpdatesToTest; i++) {
          updatePromises.push(db.tempCollection.update(queryMultiple, update, {multi: true}));
          updatePromises.push(db.tempCollection.findAndModify({query: {a: 1}, update: {$set: {'anotherProperty': 2}}}));
        }

        return Q.all(updatePromises);
      });
    })
    .then(function(saveResult) {
      actionWorked = true;
    })
    .catch(function(err) {
      console.log('Error: ');
      console.log(err);
    })
    .finally(function() {
      actionWorked.should.equal(true);
    });
  });  
});
