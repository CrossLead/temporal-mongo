import CurrentStatus from '../enum/CurrentStatus';


/**
 * Common utility functions
 */
export default {
  omit,
  sanitizeUpdate,
  delay,
  addDate,
  addCurrent,
  addRawMethods
}


/**
 * properties to remove in sanitization of update query
 */
const omitList = ['_current'];


/**
 * Exclude properties from an object
 *
 * @param  {Object} object to clone (other than exclude list)
 * @param  {Array | String} propery(ies) to exclude
 * @return {Object} Filtered object
 */
export function omit(obj, exclude) {
  const excludeSet = new Set([].concat(exclude)),
        out = {};

  for (const prop in obj) {
    if (!excludeSet.has(prop)) {
      out[prop] = obj[prop];
    }
  }

  return out;
}


/**
 * Sanitize update parameters. For example, application should not be messing with
 * the _current flag in update() or findAndModify(). If they do, likely the document
 * will get left in unusable state since tmongo manipulates it for both the original
 * doc and to-be-archived clones
 *
 * @param  {Object} update parameter passed to update() or findAndModify()
 * @return {Object} sanitized update parameter
 */
export function sanitizeUpdate(update) {
  const reduced = omit(update, omitList); // root props (e.g. the doc itself)

  Object
    .keys(reduced)
    .forEach(k => {
      const v = reduced[k];
      // $set, $setOnInsert
      if (v instanceof Object && (k === '$set' || k === '$setOnInsert')) {
        reduced[k] = omit(v, omitList);
      } else {
        reduced[k] = v;
      }
    });

  return reduced;
}


/**
 * Pause for <ms>
 *
 * @param {Integer} milliseconds to wait
 * @return Promise<undefined> promise resolving after delay
 */
export function delay(ms = 0) {
  return new Promise(res => setTimeout(res, ms));
}


/**
 * Add _current property to query object
 *
 * @param {Object} query to modify
 * @return {Object} modified query
 */
export function addCurrent(query) {
  return {
    _current: CurrentStatus.Current,
    ...query
  };
}


/**
 * Add date restriction to query
 *
 * @param {Object} query to modify
 * @return {Object} modified query
 */
export function addDate(query, date) {
  return {
    _startDate: {$lte : date},
    _endDate:   {$gt  : date},
    ...query
  };
}


/**
 * Add raw methods to prototype
 *
 * @param {Array} methods to add
 * @param {Object} (optional) options
 * @return {Function} class decorator
 */
export function addRawMethods(methods, { proxy='proxy', suffix='Raw', instance=true }={}) {

  return classToWrap => {
    const target = instance ? classToWrap.prototype : classToWrap;

    methods.forEach(method => {
      target[`${method}${suffix}`] = function(...args) {
        return this[proxy][method](...args);
      };
    });

    return classToWrap;
  }
}
