import pmongo                  from 'promised-mongo';
import PromisedMongoDatabase   from 'promised-mongo/dist/Database';
import TemporalCollection      from './classes/TemporalCollection';
import CurrentStatus           from './CurrentStatus';


export default function TemporalMongo(connectionString, collections=[], collectionConfig={}) {
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


/**
 * Add pmongo static methods / properties
 */
Object.assign(TemporalMongo, { TemporalCollection, CurrentStatus }, pmongo);
