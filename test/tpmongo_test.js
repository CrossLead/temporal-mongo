'use strict';

var tpmongo = require('../');
var should = require('should');
var Q = require('q');
describe('tpmongo', function () {

  //setup 
  var mongoCollections = ['tempCollection'];
  var db = tpmongo('localhost/mongoTestDb', mongoCollections);
  var testObjectId = db.ObjectId('95addf649ce171641a34281e');

  var setupDocuments = function() {
    var doc1 = {a: 1, _current: 1, _id: db.ObjectId('55addf649ce171641a34281d'), _rId: db.ObjectId('55addf649ce171641a34281d'), _startDate: new Date('2015-07-21 15:16:00.599Z'), _endDate: new Date('2099-07-21 15:16:00.599Z')};
    var doc2 = {a: 1, b:2, _current: 1, _id: db.ObjectId('55addf649ce171641a34281e'), _rId: db.ObjectId('55addf649ce171641a34281e'), _startDate: new Date('2015-07-21 15:16:00.599Z'), _endDate: new Date('2099-07-21 15:16:00.599Z')};
    var doc3 = {a: 1, b:3, _current: 1, _id: db.ObjectId('95addf649ce171641a34281e'), _rId: db.ObjectId('95addf649ce171641a34281e'), _startDate: new Date('2015-07-21 15:16:00.599Z'), _endDate: new Date('2099-07-21 15:16:00.599Z')};

    return db.tempCollection.removeRaw({})
    .then(function() {
      return db.tempCollection.insert(doc1);
    })
    .then(function() {
      return db.tempCollection.insert(doc2);
    })
    .then(function() {
      return db.tempCollection.insert(doc3);
    })
    .then(function() {
      return Q.delay(50);
    });
  }

  it('tpmongo should be object', function () {
		(typeof db).should.equal('object');
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

  it('save with id should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.save({_id: testObjectId, d: 4}, {upsert:true});
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

  it('save with unknown id should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.save({_id: testObjectId, d: 5}, {upsert:true});
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

  it('save with NO id should work', function () {
    var actionWorked = false;

    return setupDocuments()
    .then(function() {
      return db.tempCollection.save({d: 6}, {upsert:true});
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
      var numberOfUpdatesToTest = 3;

      var insertPromises = [];
      for(var j = 0; j < 10; j++)
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
