# tpmongo
[![NPM version][npm-image]][npm-url] [![Build Status][travis-image]][travis-url] [![Dependency Status][daviddm-url]][daviddm-image]

temporal-mongo


## Install

```bash
$ npm install --save tpmongo
```

## Usage

```javascript
import tpmongo from 'tpmongo';

const maxDate = new Date('2099-07-21 15:16:00.599Z'),
      mongoCollections = ['tempCollection'],
      db = tpmongo('localhost/tpmongoTestDb', mongoCollections, { _maxDate: maxDate });

db.tempCollection.temporalize();
```

## API

_(Coming soon)_


## Contributing

In lieu of a formal styleguide, take care to maintain the existing coding style. Add unit tests for any new or changed functionality. Lint and test your code using [gulp](http://gulpjs.com/).


## License

Copyright (c) 2016 CrossLead. Licensed under the Apache license.



[npm-url]: https://npmjs.org/package/tpmongo
[npm-image]: https://badge.fury.io/js/tpmongo.svg
[travis-url]: https://travis-ci.org/CrossLead/temporal-mongo
[travis-image]: https://travis-ci.org/CrossLead/temporal-mongo.svg?branch=master
[daviddm-url]: https://david-dm.org/CrossLead/temporal-mongo.svg?theme=shields.io
[daviddm-image]: https://david-dm.org/CrossLead/temporal-mongo
