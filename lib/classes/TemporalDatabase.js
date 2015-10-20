import pmongo                from 'promised-mongo';
import PromisedMongoDatabase from 'promised-mongo/dist/lib/Database';
import TemporalCollection    from './TemporalCollection';
import { addRawMethods }     from '../util/';


@addRawMethods([
  'collection'
])
export default class TemporalDatabase extends PromisedMongoDatabase {

  constructor(connectionString, collections, collectionConfig) {
    super(connectionString, {}, collections);

    Object.assign(this, {
      // mongo connection string
      connectionString,
      // promised-mongo raw collections
      proxyCollections:  {}
    }, pmongo)

    collections.forEach(name => new TemporalCollection(this, name, collectionConfig));
  }

  /**
   * @override
   * @param  {String} name Collection name
   * @param  {Object} [options] Options
   * @return {Collection}
   */
  collection(name/*, options*/) {
    if (this.proxyCollections && this.proxyCollections[name]) {
      return this[name];
    } else {
      return super.collection(name);
    }
  }

}
