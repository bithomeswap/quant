/* global use, db */
// MongoDB Playground
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.

// The current database to use.
use('wth000');

// Search for documents in the current collection.
db.getCollection('order余额COIN')
  .find(
    {
        symbol:"BTCUSDT"
      /*
      * Filter
      * fieldA: value or expression
      */
    },
    {
      /*
      * Projection
      * _id: 0, // exclude _id
      * fieldA: 1 // include field
      */
    }
  )
  .sort({
    日期:1
    /*
    * fieldA: 1 // ascending
    * fieldB: -1 // descending
    */
  });
