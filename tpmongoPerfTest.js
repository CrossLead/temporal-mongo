var Q = require('q');
var tpmongo  = require('./lib/tpmongo.js');
var mongoCollections = ['tempCollection'];
var db = tpmongo('localhost/mongoTestDb', mongoCollections);

var runPerformanceTest = function() {
  
  var returnObject = {success: true, errors: []};

  var tempValue = new Date();
  var doc1 = {"tempProperty": tempValue, "sameValue": 1};
  var doc2 = {"tempProperty": tempValue, "sameValue": 1, doc2val:2};
  var query = {"tempProperty": tempValue};
  var queryMultiple = {"sameValue": 1};
  var update = {$set: {"anotherProperty": 1}};
  var update2 = {$set: {"anotherProperty": 2}};
  var updateDelay = 100;
  var numberOfUpdatesToTest = 5;

  // add j * 2 documents
  var insertPromises = [];
  for(var j = 0; j < 10; j++)
  {
    insertPromises.push(db.tempCollection.insert({"tempProperty": tempValue, "sameValue": 1}, {writeConcern: "majority"}));
    insertPromises.push(db.tempCollection.insert({"tempProperty": tempValue, "sameValue": 3, doc2val:2}, {writeConcern: "majority"}));
  }

  Q.all(insertPromises)
  .then(function() {
    var updatePromises = [];
    console.time("runPerformanceTest");
    for(var i = 0; i < numberOfUpdatesToTest; i++) {
      updatePromises.push(db.tempCollection.update(queryMultiple, update, {multi: true}));
      updatePromises.push(db.tempCollection.findAndModify(queryMultiple, update));
    }

    return Q.all(updatePromises);
  })

  .then(function() {
    console.log("Everything Worked");
    console.log(JSON.stringify(returnObject));
    console.timeEnd("runPerformanceTest");
    process.exit();
  })
  .fail(function(err) {
    console.log("Process Failed: " + JSON.stringify(err));
    console.timeEnd("runPerformanceTest");
    process.exit(1)
  });
};

console.log('Starting MongoTest');
runPerformanceTest();