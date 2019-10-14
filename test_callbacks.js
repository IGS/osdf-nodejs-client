#!/usr/bin/env mocha

var osdf = require('./osdf');
var _ = require('lodash');
var assert = require('chai').assert;

osdf.setup({auth: 'test:test'});

describe('Info', function() {
    const keys = [
        'api_version', 'title', 'description', 'admin_contact_email1',
        'admin_contact_email2', 'technical_contact1', 'technical_contact2',
        'comment1', 'comment2'
    ];

    it('info', function() {
        return new Promise(function(resolve, reject) {
            osdf.info(function(err, info) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isObject(info);

                    assert.hasAllKeys(info, keys);

                    resolve();
                }
            });
        });
    });
});

describe('Nodes', function() {
    var test_node_id = null;

    var test_node = {
        ns: 'test',
        acl: { 'read': [ 'all' ], 'write': [ 'all' ] },
        linkage: {},
        node_type: 'unregistered',
        meta: {}
    };

    it('insert_node', function() {
        return new Promise(function(resolve, reject) {
            osdf.insert_node(test_node, function(err, node_id) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(node_id);
                    assert.isString(node_id);

                    // Set the node id used for other tests
                    test_node_id = node_id;

                    resolve();
                }
            });
        });
    });

    it('get_node', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_node(test_node_id, function(err, node) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(node);
                    assert.isObject(node);

                    resolve();
                }
            });
        });
    });

    it('edit_node', function() {
        return new Promise(function(resolve, reject) {
            var updated = test_node;
            updated['meta']['updated'] = true;
            updated['ver'] = 1;

            osdf.edit_node(test_node_id, updated, function(err) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNull(err);

                    resolve();
                }
            });
        });
    });

    it('get_node_by_version', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_node_by_version(test_node_id, 1, function(err, node) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(node);
                    assert.isObject(node);

                    resolve();
                }
            });
        });
    });

    it('get_node_in_links', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_node_in_links(test_node_id, function(err, links) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(links);
                    assert.isObject(links);

                    resolve();
                }
            });
        });
    });

    it('get_node_out_links', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_node_out_links(test_node_id, function(err, links) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(links);
                    assert.isObject(links);

                    resolve();
                }
            });
        });
    });

    it('delete_node', function() {
        return new Promise(function(resolve, reject) {
            osdf.delete_node(test_node_id, function(err) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNull(err);

                    resolve();
                }
            });
        });
    });

    it('validate_node', function() {
        return new Promise(function(resolve, reject) {
            osdf.validate_node(test_node, function(err, error_text) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isString(error_text);
                    assert.isEmpty(error_text);

                    resolve();
                }
            });
        });
    });

    it('validate_node (bad node)', function() {
        var bad_node = {};

        return new Promise(function(resolve, reject) {
            osdf.validate_node(bad_node, function(err, error_text) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isString(error_text);
                    assert.isNotEmpty(error_text);
                    assert.match(error_text, /acl/, 'Error mentions acl.');
                    assert.match(error_text, /meta/, 'Error mentions meta.');
                    assert.match(error_text, /node_type/, 'Error mentions node_type.');
                    assert.match(error_text, /ns/, 'Error mentions ns.');
                    assert.match(error_text, /linkage/, 'Error mentions linkage.');

                    resolve();
                }
            });
        });
    });

});

describe('Schemas', function() {
    const namespace = 'test';

    var test_schema_name = 'my_cool_name';

    var test_schema = {
        type: 'object',
        properties: {
            prop: {
                title: 'A bit of text.',
                type: 'string'
            }
        },
        additionalProperties: false,
        required: [ 'prop' ]
    };

    it('get_schema', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_schema(namespace, 'example', function(err, schema) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(schema);

                    assert.isObject(schema);

                    resolve();
                }
            });
        });
    });

    it('get_schemas (all)', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_schemas(namespace, function(err, schemas) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(schemas);

                    assert.isObject(schemas);

                    resolve();
                }
            });
        });
    });

    it('insert_schema', function() {
        return new Promise(function(resolve, reject) {
            osdf.insert_schema(namespace, test_schema_name, test_schema, function(err) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNull(err);

                    resolve();
                }
            });
        });
    });

    it('edit_schema', function() {
        return new Promise(function(resolve, reject) {
            var newer = _.clone(test_schema);
            newer['additionalProperties'] = true;

            osdf.edit_schema(namespace, test_schema_name, newer, function(err) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNull(err);

                    resolve();
                }
            });
        });
    });

    it('delete_schemas', function() {
        return new Promise(function(resolve, reject) {
            osdf.delete_schema(namespace, 'my_cool_name', function(err) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNull(err);

                    resolve();
                }
            });
        });
    });
});

describe('Aux Schemas', function() {
    const namespace = 'test';

    var aux_schema_name = 'my_cool_aux_name';

    var test_aux_schema = {
        description: 'A test aux schema.',
        type: 'object',
        properties: {
            prop: {
                title: 'A bit of text.',
                type: 'string'
            }
        },
        additionalProperties: false,
        required: [ 'prop' ]
    };

    it('get_aux_schema', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_aux_schema(namespace, 'example_aux', function(err, aux_schema) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(aux_schema);

                    assert.isObject(aux_schema);

                    resolve();
                }
            });
        });
    });

    it('get_aux_schemas (all)', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_aux_schemas(namespace, function(err, aux_schemas) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(aux_schemas);

                    assert.isObject(aux_schemas);

                    resolve();
                }
            });
        });
    });

    it('insert_aux_schema', function() {
        return new Promise(function(resolve, reject) {
            osdf.insert_aux_schema(namespace, aux_schema_name,
                test_aux_schema, function(err) {
                    if (err) {
                        assert.fail(err);
                        reject(err);
                    } else {
                        assert.isNull(err);

                        resolve();
                    }
                }
            );
        });
    });

    it('edit_aux_schema', function() {
        return new Promise(function(resolve, reject) {
            var newer = _.clone(test_aux_schema);
            newer['additionalProperties'] = true;

            osdf.edit_aux_schema(namespace, aux_schema_name,
                newer, function(err) {
                    if (err) {
                        assert.fail(err);
                        reject(err);
                    } else {
                        assert.isNull(err);

                        resolve();
                    }
                }
            );
        });
    });

    it('delete_aux_schema', function() {
        return new Promise(function(resolve, reject) {
            osdf.delete_aux_schema(namespace, 'my_cool_aux_name', function(err) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNull(err);

                    resolve();
                }
            });
        });
    });
});

describe('Namespaces', function() {
    const namespace = 'test';

    it('get_namespace', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_namespace(namespace, function(err, namespace) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(namespace);
                    assert.isObject(namespace);

                    resolve();
                }
            });
        });
    });

    it('get_namespaces (all)', function() {
        return new Promise(function(resolve, reject) {
            osdf.get_namespaces(function(err, namespaces) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(namespaces);
                    assert.isObject(namespaces);

                    resolve();
                }
            });
        });
    });
});

describe('Queries', function() {
    const namespace = 'test';

    it('query', function() {
        return new Promise(function(resolve, reject) {
            var es_query = {
                'query':{
                    'filtered':{'filter':[{'term':{'node_type':'example'}}]}
                }
            };

            var result_keys = [
                'search_result_total', 'results', 'result_count', 'page'
            ];

            osdf.query(es_query, namespace, function(err, results) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(results);

                    assert.isObject(results);

                    assert.hasAllKeys(results, result_keys);

                    assert.isNumber(results['result_count']);
                    assert.isNumber(results['search_result_total']);
                    assert.isNumber(results['page']);
                    assert.isArray(results['results']);

                    resolve();
                }
            });
        });
    });

    it('query_all', function() {
        return new Promise(function(resolve, reject) {
            var es_query = {
                'query':{
                    'filtered':{'filter':[{'term':{'node_type':'subject'}}]}
                }
            };

            var result_keys = [
                'search_result_total', 'results', 'result_count'
            ];

            osdf.query_all(es_query, namespace, function(err, results) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(results);

                    assert.isObject(results);

                    assert.hasAllKeys(results, result_keys);

                    assert.isNumber(results['result_count']);
                    assert.isNumber(results['search_result_total']);
                    assert.isArray(results['results']);

                    resolve();
                }
            });
        });
    });

    it('query_page', function() {
        var page = 1;

        var es_query = {
            'query':{
                'filtered':{'filter':[{'term':{'node_type':'example'}}]}
            }
        };

        var result_keys = [
            'search_result_total', 'results', 'result_count', 'page'
        ];

        return new Promise(function(resolve, reject) {
            osdf.query_page(es_query, namespace, page, function(err, results) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(results);

                    assert.isObject(results);

                    assert.hasAllKeys(results, result_keys);

                    assert.isNumber(results['result_count']);
                    assert.isNumber(results['search_result_total']);
                    assert.isNumber(results['page']);
                    assert.strictEqual(results['page'], page);
                    assert.isArray(results['results']);

                    resolve();
                }
            });
        });
    });

    it('oql_query', function() {
        var oql = '"example"[node_type]';

        var result_keys = [
            'search_result_total', 'results', 'result_count', 'page'
        ];

        return new Promise(function(resolve, reject) {
            osdf.oql_query(oql, namespace, function(err, results) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(results);

                    assert.isObject(results);

                    assert.hasAllKeys(results, result_keys);

                    assert.isNumber(results['result_count']);
                    assert.isNumber(results['search_result_total']);
                    assert.isNumber(results['page']);
                    assert.isArray(results['results']);

                    resolve();
                }
            });
        });
    });

    it('oql_query_all', function() {
        var oql = '"example"[node_type]';

        var result_keys = [
            'search_result_total', 'results', 'result_count'
        ];

        return new Promise(function(resolve, reject) {
            osdf.oql_query_all(oql, namespace, function(err, results) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(results);

                    assert.isObject(results);

                    assert.hasAllKeys(results, result_keys);

                    assert.isNumber(results['result_count']);
                    assert.isNumber(results['search_result_total']);
                    assert.isArray(results['results']);

                    resolve();
                }
            });

        });
    });

    it('oql_query_page', function() {
        var page = 1;

        var oql = '"example"[node_type]';

        var result_keys = [
            'search_result_total', 'results', 'result_count', 'page'
        ];

        return new Promise(function(resolve, reject) {
            osdf.oql_query_page(oql, namespace, page, function(err, results) {
                if (err) {
                    assert.fail(err);
                    reject(err);
                } else {
                    assert.isNotNull(results);

                    assert.isObject(results);

                    assert.hasAllKeys(results, result_keys);

                    assert.isNumber(results['result_count']);
                    assert.isNumber(results['search_result_total']);
                    assert.isNumber(results['page']);
                    assert.strictEqual(results['page'], page);
                    assert.isArray(results['results']);

                    resolve();
                }
            });
        });
    });
});

