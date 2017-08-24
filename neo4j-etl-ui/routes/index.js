var express = require('express'),
    router = express.Router(),
    _ = require('lodash'),
    neo4j = require('neo4j'),
    CypherBuilder = require('../lib/buildCypher'),
    babyparse = require("babyparse"),
    guidShort = require('../lib/util').guidShort,               // FIXME: better util module naming / import
    config = require('../lib/config');


// hold all data in memory to be served up for LOAD CSV
// FIXME: find a better way to persist this data
var allFiles = {};

// TODO: move route logic out into individual modules
// TODO: rename routes to something more meaningful

/** parseLoadData
 *
 * Parse load form data into datamodel mapping format
 *
 * @param formData
 * @returns {*}
 */
function parseLoadData(formData, fileData) {
  var datamodelConfig = {};
  var nodes = [], rels = [], labels = [];

  _.forEach(fileData.files, function(file) {
    _.forEach(fileData[file], function(table) {
      if (formData[table.name+'importCheck'] === 'on') {
        if (table['graph-object-type'] == 'Node') {
          var node = {};
          node['guid'] = guidShort();
          node['filename'] = file;
          node['table'] = table['name'];
          node['mapping'] = JSON.stringify(table['mappings'], null, 2).replace(/"/g, '').replace(/'/g, '');
          node['labels']  = [];
          node['properties']  = [];

          _.forEach(table['mappings'], function(mapping, pos) {
            var properties = {};
            if (mapping['field']['type'] == 'Label') {
              node['labels']  = [ mapping['column']['name'].replace(/'/g, '') ];
            } else {
              if (mapping['field']['type'] == 'Id') {
                properties['headerKey'] = 'ID';
                properties['neoKey'] = 'id';
                properties['dataType'] = table['mappings'][pos + 1]['field']['neo4j-data-type'];
                properties['primaryKey'] = 'checked';
                properties['skip'] = false;
                properties['index'] = true;
                node['idSpace'] =  mapping['field']['id-space'];
              } else if (mapping['field']['type'] == 'Data') {
                properties['headerKey'] = mapping['column']['name'];
                properties['neoKey'] = _.camelCase(mapping['field']['name']);
                properties['dataType'] = mapping['field']['neo4j-data-type'];
                properties['isArray'] = mapping['field']['is-array'];
                properties['primaryKey'] = '';
                properties['skip'] = false;
                properties['index'] = false;
              }
              node['properties'].push(properties);
            }
          });
          labels [ node['idSpace'] ] = node['labels'][0];
          nodes.push(node);

        } else if (table['graph-object-type'] == 'Relationship') {
          var rel = {};
          rel['guid'] = guidShort();
          rel['filename'] = file;
          rel['table'] = table['name'];
          rel['mapping'] = JSON.stringify(table['mappings'], null, 2).replace(/"/g, '').replace(/'/g, '');
          rel['properties'] = [];

          _.forEach(table['mappings'], function(mapping) {
            var startNode = {}, endNode = {}, properties = {};
            if (mapping['field']['type'] == 'RelationshipType') {
              rel['type'] = mapping['column']['name'].replace(/'/g, '');
            } else {
              if (mapping['field']['type'] == 'StartId') {
                startNode['table'] = mapping['field']['id-space'];
                startNode['label'] = labels[ mapping['field']['id-space'] ];
                startNode['headerKey'] = _.camelCase(mapping['column']['columns'][0]['name']);
                startNode['neoKey'] = mapping['column']['columns'][0]['name'];
                startNode['dataType'] = 'string';
                startNode['skip'] = false;
                rel['startNode'] = startNode;
              } else if (mapping['field']['type'] == 'EndId') {
                endNode['table'] = mapping['field']['id-space'];
                endNode['label'] = labels[ mapping['field']['id-space'] ];
                endNode['headerKey'] = _.camelCase(mapping['column']['columns'][0]['name']);
                endNode['neoKey'] = mapping['column']['columns'][0]['name'];
                endNode['dataType'] = 'string';
                endNode['skip'] = false;
                rel['endNode'] = endNode;
              } else if (mapping['field']['type'] == 'Data') {
                properties['headerKey'] = mapping['column']['name'];
                properties['neoKey'] = _.camelCase(mapping['field']['name']);
                properties['dataType'] = mapping['field']['neo4j-data-type'];
                properties['isArray'] = mapping['field']['is-array'];
                properties['skip'] = false;
                rel['properties'].push(properties);
              }
            }
          });
          rels.push(rel);
        }
      }
    });
  });

  datamodelConfig['nodes'] = nodes;
  datamodelConfig['relationships'] = rels;

  return datamodelConfig;
}

function toCamelCase(str) {

    var result = str;

	var isLastCharLower = false;
	var isLastCharUpper = false;
	var isLastLastCharUpper = false;

	for (var i = 0; i < result.length; i++) {
		const c = result[i];

		if (isLastCharLower && /[a-zA-Z]/.test(c) && c.toUpperCase() === c) {
			result = result.substr(0, i) + '-' + result.substr(i);
			isLastCharLower = false;
			isLastLastCharUpper = isLastCharUpper;
			isLastCharUpper = true;
			i++;
		} else if (isLastCharUpper && isLastLastCharUpper && /[a-zA-Z]/.test(c) && c.toLowerCase() === c) {
			result = result.substr(0, i - 1) + '-' + result.substr(i - 1);
			isLastLastCharUpper = isLastCharUpper;
			isLastCharUpper = false;
			isLastCharLower = true;
		} else {
			isLastCharLower = c.toLowerCase() === c;
			isLastLastCharUpper = isLastCharUpper;
			isLastCharUpper = c.toUpperCase() === c;
		}
	}

	return result;
}

function countNodesAndRels(fileData) {
  var count = 0;
  _.forEach(fileData.files, function(f) {
    count += fileData[f]['data'].length
  });
  console.log("Node + relationship count: " + count);
  return count;
}

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Express' });
});

router.get('/datamodel', function(req, res, next) {
  var fileData = req.session.fileData,
      configData = req.session.configData;

  // console.dir(fileData);
  // don't send full file data - only need fields for each file (outside of configDat)

  var fileFields = {};

  _.forEach(fileData, function(v,k) {
    if (v && v.meta && v.meta.fields) {
      fileFields[k] = v.meta.fields;
    }
  });

  // get datatypes for each field

  var fieldDatatypes = {};

  _.forEach(fileData, function(v,k) {
    if ('data' in fileData[k]) {
      var row = fileData[k]['data'][0];
      var dataTypes = {};
      _.forEach(row, function (rowv, rowk) {
        if (typeof rowv === "number" && Number.isInteger(rowv)) {
          dataTypes[rowk] = "integer"; // how to handle float vs int? using only JS Number?
        } else if (typeof rowv === "number") {
          dataTypes[rowk] = "float";
        } else if (typeof rowv === "string") {
          dataTypes[rowk] = "string";
        } else {
          dataTypes[rowk] = "string";
        }
      });
      fieldDatatypes[k] = dataTypes;
    }

  });

  var context = {};
  context['files'] = fileData.files;
  context['filesContents'] = encodeURI(JSON.stringify(fileData[fileData.files[0]]));
  context['config'] = configData;
  context['fileFields'] = fileFields;
  context['fieldDatatypes'] = fieldDatatypes;
  res.render('datamodel', context);
});

router.post('/datamodel', function(req, res, next) {
  var configData = req.body;
  req.session.configData = configData;
  req.session.save();
  res.send("OK");
});

router.get('/files/:uidparam/:filename', function(req, res, next) {
  var filename = req.params.filename,
      uidparam = req.params.uidparam;

  var data = allFiles[uidparam][filename]['data'];
  //var data = req.session.fileData[filename]['data'];

  res.set('Content-Type', 'application/csv');
  res.send(babyparse.unparse(data));
});

router.get('/import', function(req, res, next) {

  //var protocol = config.csv_file_protocol;
  //var cypherBuilder = new CypherBuilder(req.session.fileData, req.session.configData, protocol +req.headers.host, req.sessionID);


  //var csvCypher = cypherBuilder.buildCSVCypher();
  //var cypherConstraints = cypherBuilder.cypherConstraints();

  //if (countNodesAndRels(req.session.fileData) < 1000) {
    //var cypher = cypherBuilder.buildCypher();
    //res.render('import', {loadCSVCypher: csvCypher, cypherConstraints: cypherConstraints, cypher: cypher});
  //} else {
    //res.render('import', {loadCSVCypher: csvCypher, cypherConstraints: cypherConstraints});
  //}

  res.render('import', req);
});

router.post('/importNeo4jInstance', function(req, res, next) {
  // get connection vars from req.body
  // connect to Neo4j instance
  // get filedata and config data from session
  // instaantiate cypherBuilder instance and generate cypher
  // execute query against neo4j
  // return results
  var username = req.body.neo4jUser,
      password = req.body.neo4jPassword,
      neo4jURL = req.body.neo4jURL;
  var connConfig = {
    "url": neo4jURL
  };

  if (username.length > 0 && password.length > 0) {
    connConfig['auth'] = {
      "username": username,
      "password": password
    };
  }

  var db = new neo4j.GraphDatabase(connConfig);

  var cypherBuilder = new CypherBuilder(req.session.fileData, req.session.configData);

  var statements = cypherBuilder.buildCSVCypher().split(";");

  var queryObjs = [];

  _.forEach(statements, function(cypher) {

    var obj = {};
    obj['query'] = cypher;
    if (cypher.length > 1) {
      queryObjs.push(obj);
    }
  });

  db.cypher({queries: queryObjs}, function(err, results) {
    if (err) {
      console.log(err);
      //throw err;
      res.send(err);
      //return next(err);
    } else {
      res.send(results);
    }

  });

});


router.get('/load', function(req, res, next) {
  res.render('load', {title: 'Load'});
});

router.post('/load', function(req, res, next) {
  req.session.fileData = req.body;
  req.session.save();
  allFiles[req.sessionID] = req.body;
  //allFiles = req.body;
  res.send('OK');

});

router.get('/load2', function(req, res, next) {
  var fileData = req.session.fileData;
  var context = {};
  var fileInfo = [];
  var files = fileData.files;
  _.forEach(files, function(f) {
    var mapping = req.session.fileData[f];
    _.forEach(mapping, function(m) {
      var fileObj = {};
      fileObj['file'] = f;
      fileObj['name'] = m.name;
      fileObj['sql'] = m.sql;
      fileObj['mapping'] = JSON.stringify(m.mappings, null, 2);
      fileObj['node'] = m["graph-object-type"] == 'Node' ? "checked" : "disabled";
      fileObj['relationship'] = m["graph-object-type"] == 'Node' ? "disabled" : "checked";
      fileInfo.push(fileObj);
    });
  });

  context['files'] = fileInfo;
  res.render('load2', context);

});

router.post('/load2', function(req, res, next) {
  var formData = req.body;

  req.session.configData = parseLoadData(formData, req.session.fileData);
  req.session.save();

  res.redirect('/datamodel');
});

router.get('/preview/:filename', function(req, res, next) {
  var filename = req.params.filename;
  var previewData = req.session.fileData[filename];
  previewData['filename'] = filename;
  previewData['layout'] = null;
  res.render('preview', previewData)
});

router.post('/purge', function(req, res, next) {
  req.session.destroy(function(err){
    res.send('OK');
  })
});

router.get('/terms', function(req, res, next) {
  res.render('terms', {title: "Privay Policy"})
});

module.exports = router;
