'use strict';

const through = require( 'through2' );
const peliasModel = require( 'pelias-model' );

var peliasConfig = require( 'pelias-config' ).generate();


function getCentroid(record) {
  const lat = record.Latitude;
  const lon = record.Longitude;

  if (lat && lon) {
    return { lat: lat, lon: lon };
  }
}

function processRecord(record, next_uid, stats) {
  const id_number = next_uid;
  const model_prefix = "postalcode:zipcodedownload:can";
  const model_id = `${model_prefix}:${id_number}`;

  try {
    const layer = "postalcode";
    const source = "zipcodedownload";

    const pelias_document = new peliasModel.Document( source, layer, model_id );


    const centroid = getCentroid(record);
    if (centroid) {
      pelias_document.setCentroid( centroid );
    } else {
      throw 'Invalid centroid'; //centroid is required
    }

    var text = "";
    if (record.CityName) {
      text += record.CityName + ",";
    } else {
      throw 'Invalid record';
    }

    if (record.CountyName) {
      text += record.CountyName + ",";
    }

    if (record.StateName) {
      text += record.StateName +  ",";
    }

    if (record.ProvinceName) {
      text += record.ProvinceName + ",";
    }

    var zip = record.ZIPCode || record.PostalCode;
    if (zip) {
      pelias_document.setName('default', zip);
      pelias_document.setAddress('zip', zip);
    }


    if (record.CountryName) {
      text += record.CountryName;
    } else if (peliasConfig.get('imports.csv.country')){
      text += peliasConfig.imports.csv.country;
    } else {
      throw 'No country specified for zip codes'; //country is required
    }

    console.log("recordPlaceholderInput : " + text);
    pelias_document.setMeta('recordPlaceholderInput', text.toLowerCase());

    pelias_document.setAddendum('classification', { cityType : record.CityType });

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
