'use strict';

const through = require( 'through2' );

const peliasModel = require( 'pelias-model' );

function getCentroid(record) {
  const lat = record.Latitude;
  const lon = record.Longitude;

  if (lat && lon) {
    return { lat: lat, lon: lon };
  }
}

function processRecord(record, next_uid, stats) {
  const id_number = next_uid;
  const model_prefix = "pobox:zipcodes";
  const model_id = `${model_prefix}:${id_number}`;

  try {
    const layer = "postalcode";
    const source = "zipcodes";

    const pelias_document = new peliasModel.Document( source, layer, model_id );

    if (record.ZipCode) {
      pelias_document.setName('default', record.ZipCode);
      pelias_document.setAddress('zip', record.ZipCode);
    }

    const centroid = getCentroid(record);
    if (centroid) {
      pelias_document.setCentroid( centroid );
    } else {
      throw 'Invalid centroid'; //centroid is required
    }

    var city = "";
    if (record.City) {
      city += record.City + ",";
    }

    var cityAlias = "";
    if (record.CityAliasName) {
      cityAlias += record.CityAliasName + ",";
    }

    var text = "";
    if (record.County) {
      text += record.County + ",";
    }

    if (record.State) {
      text += record.State +  ",";
    }

    if (record.Country) {
      text += record.Country;
    } 

    pelias_document.setMeta('recordPlaceholderInput', (city + text).toLowerCase());
    if (record.CityAliasName && record.CityAliasName !== record.City) {
      pelias_document.setMeta('recordPlaceholderInput_1', (cityAlias + text).toLowerCase());
    }

    return pelias_document;
  } catch ( ex ){
    stats.badRecordCount++;
  }
}

/*
 * Create a stream of Documents from valid, cleaned CSV records
 */
function createDocumentStream(id_prefix, stats) {
  /**
   * Used to track the UID of individual records passing through the stream if
   * there is no HASH that can be used as a more unique identifier.  See
   * `peliasModel.Document.setId()` for information about UIDs.
   */
  let uid = 0;

  return through.obj(
    function write( record, enc, next ){
      const pelias_document = processRecord(record, uid, stats);
      uid++;

      if (pelias_document) {
        this.push( pelias_document );
      }

      next();
    }
  );
}

module.exports = {
  create: createDocumentStream
};
