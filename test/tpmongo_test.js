'use strict';

var tpmongo = require('../');
var assert = require('should');
var Q = require('q');
describe('tpmongo', function () {

  it('tpmongo should be object', function () {
		var mongoCollections = ['tempCollection'];
		var db = tpmongo('localhost/mongoTestDb', mongoCollections);
	    
		(typeof db).should.equal('object');
  });
  
});
