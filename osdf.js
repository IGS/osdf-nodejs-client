var async = require('async');
var _ = require('lodash');

/**
 * This is the OSDF JavaScript client to expose the OSDF REST API as an easy
 * to use library. The full range of OSDF capabilities regarding Nodes,
 * Namespaces, Schemas, etc, is implemented. In addition, this module is
 * designed to be used with either traditinal error callbacks, or with the newer
 * async/await/promise based coding style.
 *
 * For example:
 *
 * // Traditional error callback
 * osdf.info(function(err, info) {
 *     if (err) {
 *         console.error(err);
 *     } else {
 *         console.log(info);
 *     }
 * });
 *
 * or
 *
 * // Async/Await/Promises:
 * var info = await osdf.info();
 */

/** The name of the module. */
var osdf = exports;

var http =  null;

osdf.host = '127.0.0.1';
osdf.port = 8123;
osdf.auth = null;
osdf.ssl = false;

/**
 * Deletes an auxiliary schema from the OSDF server.
 * @param {string} namespace - The namespace to remove the auxiliary
 * schema from.
 * @param {string} aux_schema_name - The name of the auxiliary schema to delete.
 * @param {Function} [callback] - A callback which is called when the deletion
 * is completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.delete_aux_schema = function(namespace, aux_schema_name, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/aux/' + aux_schema_name,
        method: 'DELETE'
    };

    return deletion_helper(options, callback);
};

/**
 * Deletes a node document from the OSDF server.
 * @param {string} node_id - The ID of the node to delete.
 * @param {Function} [callback] - A callback which is called when the deletion
 * is completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.delete_node = function(node_id, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/' + node_id,
        method: 'DELETE'
    };

    return deletion_helper(options, callback);
};

/**
 * Deletes a schema from the OSDF server.
 * @param {string} namespace - The namespace to remove the schema from.
 * @param {string} schema_name - The name of the schema to delete.
 * @param {Function} [callback] - A callback which is called when the deletion
 * is completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.delete_schema = function(namespace, schema_name, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/' + schema_name,
        method: 'DELETE'
    };

    return deletion_helper(options, callback);
};

/**
 * Updates (edits) an existing auxiliary schema.
 * @param {string} namespace - The namespace the auxiliary schema exists in.
 * @param {string} aux_schema_name - The name of the auxiliary schema to edit.
 * @param {Object} aux_schema - The new JSON-Schema object.
 * @param {Function} [callback] - A callback which is called when the edit is
 * completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.edit_aux_schema = function(namespace, aux_schema_name, aux_schema, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/aux/' + aux_schema_name,
        method: 'PUT'
    };

    if (_.isFunction(callback)) {
        var cb = function(response) {
            response.on('data', function(chunk) {
                // ignored
            });
            response.on('end', function() {
                if (response['statusCode'] === 200) {
                    callback(null);
                } else {
                    if (response['headers'].hasOwnProperty('x-osdf-error')) {
                        callback(response['headers']['x-osdf-error']);
                    } else if (response.hasOwnProperty('statusMessage'))  {
                        callback(response['statusMessage']);
                    } else {
                        callback(response['statusCode']);
                    }
                }
            });
        };

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.write(JSON.stringify(aux_schema));

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = function(response) {
                response.on('data', function(chunk) {
                    // ignored
                });
                response.on('end', function() {
                    if (response['statusCode'] === 200) {
                        resolve();
                    } else {
                        if (response['headers'].hasOwnProperty('x-osdf-error')) {
                            reject(response['headers']['x-osdf-error']);
                        } else if (response.hasOwnProperty('statusMessage'))  {
                            reject(response['statusMessage']);
                        } else {
                            reject(response['statusCode']);
                        }
                    }
                });
            };

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.write(JSON.stringify(aux_schema));

            request.end();
        });
    }
};

/**
 * Updates (edits) an existing OSDF node document.
 * @param {string} node_id - The ID of the node to update/edit.
 * @param {Object} node_data - The new object to replace the existing data with.
 * @param {Function} [callback] - A callback which is called when the edit is
 * completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.edit_node = function(node_id, node_data, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/' + node_id,
        method: 'PUT'
    };

    if (_.isFunction(callback)) {
        var cb = function(response) {
            response.on('data', function(chunk) {
                // ignored
            });
            response.on('end', function() {
                if (response['statusCode'] === 200) {
                    callback(null);
                } else {
                    if (response['headers'].hasOwnProperty('x-osdf-error')) {
                        callback(response['headers']['x-osdf-error']);
                    } else if (response.hasOwnProperty('statusMessage'))  {
                        callback(response['statusMessage']);
                    } else {
                        callback(response['statusCode']);
                    }
                }
            });
        };

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.write(JSON.stringify(node_data));

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = function(response) {
                response.on('data', function(chunk) {
                    // ignored
                });
                response.on('end', function() {
                    if (response['statusCode'] === 200) {
                        resolve();
                    } else {
                        if (response['headers'].hasOwnProperty('x-osdf-error')) {
                            reject(response['headers']['x-osdf-error']);
                        } else if (response.hasOwnProperty('statusMessage'))  {
                            reject(response['statusMessage']);
                        } else {
                            reject(response['statusCode']);
                        }
                    }
                });
            };

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.write(JSON.stringify(node_data));

            request.end();
        });
    }
};

/**
 * Updates (edits) an existing OSDF schema. Future node insertions and edits to
 * nodes with a node_type matching the name of the schema, must comply with
 * rules and specifications of the new schema document.
 * @param {string} namespace - The namespace that the existing schema is in.
 * @param {string} name - The name of the schema to update/edit.
 * @param {Object} schema - The new schema object to replace the existing schema
 * with.
 * @param {Function} [callback] - A callback which is called when the edit is
 * completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.edit_schema = function(namespace, name, schema, callback ) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/' + name,
        method: 'PUT'
    };

    var schema_doc = {
        name: name,
        schema: schema
    };

    if (_.isFunction(callback)) {
        var cb = function(response) {
            response.on('data', function(chunk) {
                // ignored
            });
            response.on('end', function() {
                if (response['statusCode'] === 200) {
                    callback(null);
                } else {
                    if (response['headers'].hasOwnProperty('x-osdf-error')) {
                        callback(response['headers']['x-osdf-error']);
                    } else if (response.hasOwnProperty('statusMessage'))  {
                        callback(response['statusMessage']);
                    } else {
                        callback(response['statusCode']);
                    }
                }
            });
        };

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.write(JSON.stringify(schema_doc));

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = function(response) {
                response.on('data', function(chunk) {
                    // ignored
                });
                response.on('end', function() {
                    if (response['statusCode'] === 200) {
                        resolve();
                    } else {
                        if (response['headers'].hasOwnProperty('x-osdf-error')) {
                            reject(response['headers']['x-osdf-error']);
                        } else if (response.hasOwnProperty('statusMessage'))  {
                            reject(response['statusMessage']);
                        } else {
                            reject(response['statusCode']);
                        }
                    }
                });
            };

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.write(JSON.stringify(schema_doc));

            request.end();
        });
    }
};

/**
 * Retrieves an existing OSDF auxiliary schema.
 * @param {string} namespace - The namespace that the auxiliary schema is in.
 * @param {string} aux_schema_name - The name of the schema to retrieve.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_aux_schema = function(namespace, aux_schema_name, callback ) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/aux/' + aux_schema_name,
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves all the auxiliary schemas belonging to a given namespace.
 * @param {string} namespace - The namespace to retrieve the auxiliary schemas
 * from.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_aux_schemas = function(namespace, callback ) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/aux/',
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves information about a namespace.
 * @param {string} namespace - The name of the namespace to retrieve metadata
 * about.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_namespace = function(namespace, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace,
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves information about all the namespaces registerd in the server.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_namespaces = function(callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces',
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves a node document by ID.
 * @param {string} node_id - The ID of the node document to fetch.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_node = function(node_id, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/' + node_id,
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves a specific version of node document.
 * @param {string} node_id - The ID of the node document to fetch.
 * @param {number} version - The version number of the document to fetch.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_node_by_version = function(node_id, version, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/' + node_id + '/ver/' + version,
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves the list of node documents that the specified node has inbound
 * links from (the nodes that connect TO this node).
 * @param {string} node_id - The ID of the node document to fetch.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_node_in_links = function(node_id, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/' + node_id + '/in',
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves the list of node documents that the specified node has outbound
 * links to (the nodes that this node connect TO).
 * @param {string} node_id - The ID of the node document to fetch.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_node_out_links = function(node_id, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/' + node_id + '/out',
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves a specific schema document belonging to a namespace.
 * @param {string} namespace - The name of the namespace the schema is in.
 * @param {string} schema_name - The name of the schema to retrieve.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_schema = function(namespace, schema_name, callback ) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/' + schema_name,
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves all of the schema documents belonging to a given namespace.
 * @param {string} namespace - The namespace to fetch the schemas from.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.get_schemas = function(namespace, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/',
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Retrieves metadata information about the OSDF server, including
 * administrative and technical contact information.
 * @param {Function} [callback] - A callback which is called when the retrieval
 * is completed. Invoked with (err, data).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.info = function(callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/info',
        method: 'GET'
    };

    return get_helper(options, callback);
};

/**
 * Creates (inserts) a new auxiliary schema into a given namespace.
 * @param {string} namespace - The namespace to register the auxiliary schema
 * with.
 * @param {string} name - The name to give to the auxiliary schema.
 * @param {Object} aux_schema - The schema document to use for the auxiliary
 * schema.
 * @param {Function} [callback] - A callback which is called when the insertion
 * is completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.insert_aux_schema = function(namespace, name, aux_schema, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas/aux',
        method: 'POST'
    };

    var aux_schema_doc = {
        name: name,
        schema: aux_schema
    };

    return insertion_helper(options, aux_schema_doc, callback);
};

/**
 * Creates (inserts) a new node document.
 * @param {Object} node_data - The node document to insert/create.
 * schema.
 * @param {Function} [callback] - A callback which is called when the insertion
 * is completed. Invoked with (err, node_id).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.insert_node = function(node_data, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes',
        method: 'POST'
    };

    if (_.isFunction(callback)) {
        var cb = function(response) {
            response.on('data', function(chunk) {
                // ignored
            });

            response.on('end', function() {
                if (response['statusCode'] === 201) {
                    var location = response['headers']['location'];
                    var node_id = location.split('/').pop();
                    callback(null, node_id);
                } else {
                    if (response['headers'].hasOwnProperty('x-osdf-error')) {
                        callback(response['headers']['x-osdf-error'], null);
                    } else if (response.hasOwnProperty('statusMessage'))  {
                        callback(response['statusMessage'], null);
                    } else {
                        callback(response['statusCode'], null);
                    }
                }
            });
        };

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.write(JSON.stringify(node_data));

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = function(response) {
                response.on('data', function(chunk) {
                    // ignored
                });

                response.on('end', function() {
                    if (response['statusCode'] === 201) {
                        var location = response['headers']['location'];
                        var node_id = location.split('/').pop();

                        resolve(node_id);
                    } else {
                        if (response['headers'].hasOwnProperty('x-osdf-error')) {
                            reject(response['headers']['x-osdf-error']);
                        } else if (response.hasOwnProperty('statusMessage'))  {
                            reject(response['statusMessage']);
                        } else {
                            reject(response['statusCode']);
                        }
                    }
                });
            };

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.write(JSON.stringify(node_data));

            request.end();
        });
    }
};

/**
 * Creates (inserts) a new schema document.
 * @param {string} namespace - The namespace to register the new schema with.
 * @param {string} name - The name to give to the new schema.
 * @param {Object} schema - The schema document to insert/create.
 * @param {Function} [callback] - A callback which is called when the insertion
 * is completed. Invoked with (err).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.insert_schema = function(namespace, name, schema, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/namespaces/' + namespace + '/schemas',
        method: 'POST'
    };

    var schema_doc = {
        name: name,
        schema: schema
    };

    return insertion_helper(options, schema_doc, callback);
};

/**
 * Perform a search with an OQL (OSDF Query Language) and retrieve the results.
 * @param {string} oql_query - The OQL query to perform.
 * @param {string} namespace - The namespace to query nodes from.
 * @param {Function} [callback] - A callback which is called when the search
 * is completed. Invoked with (err, results).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.oql_query = function(oql_query, namespace, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/oql/' + namespace,
        method: 'POST'
    };

    return query_helper(options, oql_query, callback);
};

/**
 * Perform a search with an OQL (OSDF Query Language) and retrieve all the
 * results, fetching each batch of results page by page, if necessary. These
 * types of queries can take more time and also require more memory. If the
 * number of pages in search results is large, it may be better to page through
 * them oneself.
 * @param {string} oql_query - The OQL query to perform.
 * @param {string} namespace - The namespace to query nodes from.
 * @param {Function} [callback] - A callback which is called when the search
 * is completed. Invoked with (err, results).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.oql_query_all = function(oql_query, namespace, callback) {
    var helper = null;

    if (_.isFunction(callback)) {
        helper = make_query_all_callback_helper();
        helper(oql_query, namespace, osdf, callback);
    } else {
        helper = make_query_all_promise_helper(oql_query);
        return helper(oql_query, namespace, osdf);
    }
};

/**
 * Perform a search with an OQL (OSDF Query Language) and retrieve a specific
 * page (or batch) of results. The OSDF server yields paginated search results
 * so this allows the caller to page through these.
 * @param {string} oql_query - The OQL query to perform.
 * @param {string} namespace - The namespace to query nodes from.
 * @param {number} page - The specific page of results to retrieve.
 * @param {Function} [callback] - A callback which is called when the search
 * is completed. Invoked with (err, results).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.oql_query_page = function(oql_query, namespace, page, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/oql/' + namespace + '/page/' + page,
        method: 'POST'
    };

    return query_helper(options, oql_query, callback);
};

/**
 * Perform a search with an elasticsearch query (express with JSON).
 * @param {string} es_query - The ElasticSearch DSL query to perform.
 * @param {string} namespace - The namespace to query nodes from.
 * @param {Function} [callback] - A callback which is called when the search
 * is completed. Invoked with (err, results).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.query = function(es_query, namespace, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/query/' + namespace,
        method: 'POST'
    };

    return query_helper(options, es_query, callback);
};

/**
 * Perform a search with an elasticsearch query (express with JSON).
 * @param {string} es_query - The ElasticSearch DSL query to perform.
 * @param {string} namespace - The namespace to query nodes from.
 * @param {Function} [callback] - A callback which is called when the search
 * is completed. Invoked with (err, results).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.query_all = function(es_query, namespace, callback) {
    var helper = null;

    if (_.isFunction(callback)) {
        helper  = make_query_all_callback_helper(es_query);
        helper(es_query, namespace, osdf, callback);
    } else {
        helper = make_query_all_promise_helper(es_query);
        return helper(es_query, namespace, osdf);
    }
};

/**
 * Perform a search with an elasticsearch query (express with JSON). The OSDF
 * server yields paginated search results so this allows the caller to page
 * through these.
 * @param {string} es_query - The ElasticSearch DSL query to perform.
 * @param {string} namespace - The namespace to query nodes from.
 * @param {number} page - The specific page of results to retrieve.
 * @param {Function} [callback] - A callback which is called when the search
 * is completed. Invoked with (err, results).
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.query_page = function(es_query, namespace, page, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/query/' + namespace + '/page/' + page,
        method: 'POST'
    };

    return query_helper(options, es_query, callback);
};

/**
 * Configure the client with the details of how to connect to the OSDF server.
 *
 * Supported settings are:
 *   {
 *     "host": "server hostname or IP address",
 *     "port": "TCP port (8123 by default)",
 *     "auth": "username:password",
 *     "ssl":  False
 *   }
 *
 * @param {Object} setting - The object containing the server details to use.
 * @returns {Object} The client object.
 */
osdf.setup = function(settings) {
    this.host = settings.host;
    this.ssl = settings.ssl;

    // Are we using TLS/SSL or not? This controls whether we use the
    // http or https module.
    if (this.ssl) {
        http = require('https');
    } else {
        http = require('http');
    }

    if (settings['port']) {
        this.port = parseInt(settings.port, 10);
    }

    if (settings['auth']) {
        this.auth = settings.auth;
    }

    return this;
};

/**
 * Determine if a document is compliant with a node's required structure as well
 * as any schema that may be associated with nodes of its type.
 * @param {Object} node_data - The node document to be checked.
 * @param {Function} [callback] - A callback which is called when the validation
 * has completed. Invoked with (err, validation_text), where validation_text
 * contains any errors the validation process discovered.
 * @returns {Promise} A promise, if a callback is omitted.
 */
osdf.validate_node = function(node_data, callback) {
    var options = {
        auth: this.auth,
        host: this.host,
        port: this.port,
        path: '/nodes/validate',
        method: 'POST'
    };

    if (_.isFunction(callback)) {
        var cb = function(response) {
            var body = '';

            response.on('data', function(chunk) {
                body = body + chunk;
            });

            response.on('end', function() {
                // 422 node data was INVALID
                // 200 node data was valida
                if (response['statusCode'] === 422 ||
                    response['statusCode'] === 200) {
                    callback(null, body);
                } else {
                    if (response['headers'].hasOwnProperty('x-osdf-error')) {
                        callback(response['headers']['x-osdf-error'], null);
                    } else if (response.hasOwnProperty('statusMessage'))  {
                        callback(response['statusMessage'], null);
                    } else {
                        callback(response['statusCode'], null);
                    }
                }
            });
        };

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.write(JSON.stringify(node_data));

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = function(response) {
                var body = '';

                response.on('data', function(chunk) {
                    body = body + chunk;
                });

                response.on('end', function() {
                    // 422 node data was INVALID
                    // 200 node data was valida
                    if (response['statusCode'] === 422 ||
                        response['statusCode'] === 200) {
                        resolve(body);
                    } else {
                        if (response['headers'].hasOwnProperty('x-osdf-error')) {
                            reject(response['headers']['x-osdf-error']);
                        } else if (response.hasOwnProperty('statusMessage'))  {
                            reject(response['statusMessage']);
                        } else {
                            reject(response['statusCode']);
                        }
                    }
                });
            };

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.write(JSON.stringify(node_data));

            request.end();
        });
    }
};

function get_helper(options, callback) {
    if (_.isFunction(callback)) {
        var cb = make_regular_cb(callback);

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = make_promise_cb(resolve, reject);

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.end();
        });
    }
}

function deletion_helper(options, callback) {
    if (_.isFunction(callback)) {
        var cb = function(response) {
            response.on('data', function(chunk) {
                // ignored
            });
            response.on('end', function() {
                if (response['statusCode'] === 204) {
                    callback(null);
                } else {
                    if (response['headers'].hasOwnProperty('x-osdf-error')) {
                        callback(response['headers']['x-osdf-error']);
                    } else if (response.hasOwnProperty('statusMessage'))  {
                        callback(response['statusMessage']);
                    } else {
                        callback(response['statusCode']);
                    }
                }
            });
        };

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = function(response) {
                response.on('data', function(chunk) {
                    // ignored
                });
                response.on('end', function() {
                    if (response['statusCode'] === 204) {
                        resolve();
                    } else {
                        if (response['headers'].hasOwnProperty('x-osdf-error')) {
                            reject(response['headers']['x-osdf-error']);
                        } else if (response.hasOwnProperty('statusMessage'))  {
                            reject(response['statusMessage']);
                        } else {
                            reject(response['statusCode']);
                        }
                    }
                });
            };

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.end();
        });
    }
}

function insertion_helper(options, data, callback) {
    if (_.isFunction(callback)) {
        var cb = function(response) {
            response.on('data', function(chunk) {
                // ignored
            });
            response.on('end', function() {
                if (response['statusCode'] === 201) {
                    callback(null);
                } else {
                    if (response['headers'].hasOwnProperty('x-osdf-error')) {
                        callback(response['headers']['x-osdf-error'], null);
                    } else if (response.hasOwnProperty('statusMessage'))  {
                        callback(response['statusMessage'], null);
                    } else {
                        callback(response['statusCode'], null);
                    }
                }
            });
        };

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        request.write(JSON.stringify(data));

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = function(response) {
                response.on('data', function(chunk) {
                    // ignored
                });
                response.on('end', function() {
                    if (response['statusCode'] === 201) {
                        resolve();
                    } else {
                        if (response['headers'].hasOwnProperty('x-osdf-error')) {
                            reject(response['headers']['x-osdf-error']);
                        } else if (response.hasOwnProperty('statusMessage')) {
                            reject(response['statusMessage']);
                        } else {
                            reject(response['statusCode']);
                        }
                    }
                });
            };

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            request.write(JSON.stringify(data));

            request.end();
        });
    }
}

/**
 * A private helper function to assist in construction of a traditional error
 * callback to indicate when work has completed.
 * @param {Function} callback - The Promise resolve() function.
 * @returns {Function} A function that accepts the HTTP response object and
 * uses the given callback to signal success or failure by invoking it with
 * (err, data).
 */
function make_regular_cb(callback) {
    return function(response) {
        var body = '';

        response.on('data', function(chunk) {
            body = body + chunk;
        });

        response.on('end', function() {
            if (response['statusCode'] >= 200 && response['statusCode'] < 300) {
                callback(null, JSON.parse(body));
            } else {
                if (response['headers'].hasOwnProperty('x-osdf-error')) {
                    callback(response['headers']['x-osdf-error'], null);
                } else if (response.hasOwnProperty('statusMessage'))  {
                    callback(response['statusMessage'], null);
                } else {
                    callback(response['statusCode'], null);
                }
            }
        });
    };
}

/**
 * A private helper function to assist in construction of callback that uses
 * promises to resolve/reject the work to be performed.
 * @param {Function} resolve - The Promise resolve() function.
 * @param {Function} reject - The Promise reject() function.
 * @returns {Function} A callback that uses resolve/reject semantics for
 * completion, depending on such things as the HTTP response status code.
 */
function make_promise_cb(resolve, reject) {
    return function(response) {
        var body = '';

        response.on('data', function(chunk) {
            body = body + chunk;
        });

        response.on('end', function() {
            if (response['statusCode'] >= 200 && response['statusCode'] < 300) {
                resolve(JSON.parse(body));
            } else {
                if (response['headers'].hasOwnProperty('x-osdf-error')) {
                    reject(response['headers']['x-osdf-error']);
                } else if (response.hasOwnProperty('statusMessage'))  {
                    reject(response['statusMessage']);
                } else {
                    reject(response['statusCode']);
                }
            }
        });
    };
}

function make_query_all_callback_helper() {
    return function(query, namespace, osdf, callback) {
        var page = 1;
        var has_next_page = true;
        var all_results = [];

        var func;
        if (_.isString(query)) {
            func = osdf.oql_query_page.bind(osdf);
        } else {
            func = osdf.query_page.bind(osdf);
        }

        async.doWhilst(
            function(cb) {
                func(query, namespace, page, function(err, page_result) {
                    if (err) {
                        cb(err);
                    } else {
                        page++;

                        if (page_result['results'].length > 0) {
                            all_results = all_results.concat(page_result['results']);
                        } else {
                            has_next_page = false;
                        }

                        cb(null, all_results);
                    }
                });
            },
            function(results, cb) {
                cb(null, has_next_page);
            },
            function(err, results) {
                if (err) {
                    callback(err, null);
                } else {
                    var final = {
                        'search_result_total': all_results.length,
                        'result_count': all_results.length,
                        'results': all_results
                    };

                    callback(null, final);
                }
            }
        );
    };
}

function make_query_all_promise_helper() {
    return function(query, namespace, osdf) {
        return new Promise(function(resolve, reject) {
            var has_next_page = true;
            var all_results = [];
            var page = 1;

            var func;
            if (_.isString(query)) {
                func = osdf.oql_query_page.bind(osdf);
            } else {
                func = osdf.query_page.bind(osdf);
            }

            async.doWhilst(
                function(cb) {
                    func(query, namespace, page, function(err, page_result) {
                        if (err) {
                            cb(err);
                        } else {
                            page++;

                            if (page_result['results'].length > 0) {
                                all_results = all_results.concat(page_result['results']);
                            } else {
                                has_next_page = false;
                            }

                            cb(null, all_results);
                        }
                    });
                },
                function(results, cb) {
                    cb(null, has_next_page);
                },
                function(err, results) {
                    if (err) {
                        reject(err);
                    } else {
                        var final = {
                            'search_result_total': all_results.length,
                            'result_count': all_results.length,
                            'results': all_results
                        };

                        resolve(final);
                    }
                }
            );
        });
    };
}

/**
 * A private helper function used by the public OQL and ES query functions.
 * @param {Object} options - An options object used by the http module.
 * @param {Object|string} query - An ES (JSON) or OQL (string) query.
 * @param {Function} [callback] - A callback which is called when the query
 * has completed. Invoked with (err, results).
 * @returns {Promise} A promise, if a callback is omitted.
 */
function query_helper(options, query, callback) {
    if (_.isFunction(callback)) {
        var cb = make_regular_cb(callback);

        var request = http.request(options, cb);

        request.on('error', function(err) {
            callback(err, null);
        });

        if (_.isObject(query)) {
            request.write(JSON.stringify(query));
        } else {
            request.write(query);
        }

        request.end();
    } else {
        return new Promise(function(resolve, reject) {
            var cb = make_promise_cb(resolve, reject);

            var request = http.request(options, cb);

            request.on('error', function(err) {
                reject(err);
            });

            // This should handle correct writing of the query regardless of
            // whether it was in ES format, or an OQL string.
            if (_.isObject(query)) {
                request.write(JSON.stringify(query));
            } else {
                request.write(query);
            }

            request.end();
        });
    }
}
