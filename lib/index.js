import pmongo                  from 'promised-mongo';
import TemporalCollection      from './classes/TemporalCollection';
import TemporalDatabase        from './classes/TemporalDatabase';
import CurrentStatus           from './enum/CurrentStatus';
import util                    from './util/';


export default function TemporalMongo(connectionString, collections=[], collectionConfig={}) {
  return new TemporalDatabase(connectionString, collections, collectionConfig);
};


/**
 * Add pmongo static methods / properties
 */
Object.assign(TemporalMongo, {
  TemporalCollection,
  TemporalDatabase,
  CurrentStatus,
  util
}, pmongo);
