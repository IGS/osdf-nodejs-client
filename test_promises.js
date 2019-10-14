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

    it('info', async function() {
        var info;
        var err = null;

        try {
            info = await osdf.info();
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isObject(info);
                assert.hasAllKeys(info, keys);

                resolve();
            }
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

    it('insert_node', async function() {
        var err = null;
        var node_id = null;

        try {
            node_id = await osdf.insert_node(test_node);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNotNull(node_id);
                assert.isString(node_id);

                test_node_id = node_id;

                resolve();
            }
        });
    });

    it('get_node', async function() {
        var node = null;
        var err = null;
        var required_keys = [
            'acl', 'hash', 'id', 'linkage', 'node_type', 'ns', 'ver', 'meta'
        ];

        try {
            node = await osdf.get_node(test_node_id);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNotNull(node);
                assert.hasAllKeys(node, required_keys);

                resolve();
            }
        });
    });

    it('edit_node', async function() {
        var err = null;

        var updated = _.clone(test_node);
        updated['meta']['updated'] = true;
        updated['ver'] = 1;

        try {
            await osdf.edit_node(test_node_id, updated);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
             assert.isNull(err);
        }, err);
    });

    it('get_node_by_version', async function() {
        var node = null;
        var err = null;
        var version = 1;

        try {
            node = await osdf.get_node_by_version(test_node_id, version);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
             assert.isNotNull(node);
             assert.isNumber(node['ver']);
             assert.strictEqual(version, node['ver']);
        }, err);
    });

    it('get_node_in_links', async function() {
        var err = null;
        var links = null;

        try {
            links = await osdf.get_node_in_links(test_node_id);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isNotNull(links);
            assert.hasAllKeys(links, ['page', 'results', 'result_count']);
            assert.isArray(links['results']);
            assert.isNumber(links['page']);
            assert.isNumber(links['result_count']);
        }, err);
    });

    it('get_node_out_links', async function() {
        var err = null;
        var links = null;

        try {
            links = await osdf.get_node_out_links(test_node_id);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isNotNull(links);
            assert.hasAllKeys(links, ['page', 'results', 'result_count']);
            assert.isArray(links['results']);
            assert.isNumber(links['page']);
            assert.isNumber(links['result_count']);
        }, err);
    });

    it('delete_node', async function() {
        var err = null;

        try {
            await osdf.delete_node(test_node_id);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isNull(err);
        }, err);
    });

    it('validate_node', async function() {
        var error_text = null;
        var err = null;

        try {
            error_text = await osdf.validate_node(test_node);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isString(error_text);
            assert.isEmpty(error_text);
        }, err);
    });

    it('validate_node (bad node)', async function() {
        var bad_node = {};
        var error_text = null;
        var err = null;

        try {
            error_text = await osdf.validate_node(bad_node);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isString(error_text);
            assert.isNotEmpty(error_text);
            assert.match(error_text, /acl/, 'Error mentions acl.');
            assert.match(error_text, /meta/, 'Error mentions meta.');
            assert.match(error_text, /node_type/, 'Error mentions node_type.');
            assert.match(error_text, /ns/, 'Error mentions ns.');
            assert.match(error_text, /linkage/, 'Error mentions linkage.');
        }, err);
    });
});

describe('Schemas', function() {
    var test_namespace = 'test';
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


    it('get_schema', async function() {
        var schema = null;
        var err = null;

        try {
            schema = await osdf.get_schema(test_namespace, 'example');
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isNotNull(schema);
            assert.isObject(schema);
        }, err);
    });

    it('get_schemas (all)', async function() {
        var schemas = null;
        var err = null;

        try {
            schemas = await osdf.get_schemas('test2');
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isNotNull(schemas);
            assert.isObject(schemas);
        }, err);
    });

    it('insert_schema', async function() {
        var err = null;

        try {
            await osdf.insert_schema(test_namespace, test_schema_name, test_schema);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isNull(err);
        }, err);
    });

    it('edit_schema', async function() {
        var err = null;

        try {
            var newer = _.clone(test_schema);
            newer['additionalProperties'] = true;

            await osdf.edit_schema(test_namespace, test_schema_name, newer);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail(err);
                reject(err);
            } else {
                assert.isNull(err);

                resolve();
            }
        });
    });

    it('delete_schema', async function() {
        var err = null;

        try {
            await osdf.delete_schema(test_namespace, test_schema_name);
        } catch (exception) {
            err = exception;
        }

        return mk_assertion_promise(function() {
            assert.isNull(err);
        }, err);
    });
});

describe('Aux Schemas', function() {
    var test_namespace = 'test';
    var aux_schema_name = 'my_cool_name';

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

    it('get_aux_schema', async function() {
        var err = null;
        var aux_schema = null;

        try {
            aux_schema = await osdf.get_aux_schema(test_namespace, 'example_aux');
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNotNull(aux_schema);
                assert.isObject(aux_schema);

                resolve();
            }
        });
    });

    it('get_aux_schemas (all)', async function() {
        var err = null;
        var aux_schemas = null;

        try {
            aux_schemas = await osdf.get_aux_schemas(test_namespace);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNotNull(aux_schemas);
                assert.isObject(aux_schemas);

                resolve();
            }
        });
    });

    it('insert_aux_schema', async function() {
        var err = null;

        try {
            await osdf.insert_aux_schema(test_namespace, aux_schema_name,
                test_aux_schema);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNull(err);

                resolve();
            }
        });
    });

    it('edit_aux_schema', async function() {
        var err = null;

        try {
            var newer = _.clone(test_aux_schema);
            newer['additionalProperties'] = true;

            await osdf.edit_aux_schema(test_namespace, aux_schema_name, newer);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail(err);
                reject(err);
            } else {
                assert.isNull(err);

                resolve();
            }
        });
    });

    it('delete_aux_schema', async function() {
        var err = null;

        try {
            await osdf.delete_aux_schema(test_namespace, aux_schema_name);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNull(err);

                resolve();
            }
        });
    });
});

describe('Namespaces', function() {
    it('get_namespace', async function() {
        var err = null;
        var namespace = null;

        try {
            namespace = await osdf.get_namespace('test');
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNotNull(namespace);
                assert.isObject(namespace);

                resolve();
            }
        });
    });

    it('get_namespaces (all)', async function() {
        var err = null;
        var namespaces = null;

        try {
            namespaces = await osdf.get_namespaces();
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
                reject(err);
            } else {
                assert.isNotNull(namespaces);
                assert.isObject(namespaces);

                resolve();
            }
        });
    });
});

describe('Queries', function() {
    const namespace = 'test';

    it('query', async function() {
        var err = null;

        var es_query = {
            'query':{
                'filtered':{'filter':[{'term':{'node_type':'example'}}]}
            }
        };

        var result_keys = [
            'search_result_total', 'results', 'result_count', 'page'
        ];

        try {
            results = await osdf.query(es_query, namespace);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
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

    it('query_all', async function() {
        var err = null;
        var results = null;
        var es_query = {
            'query':{
                'filtered':{'filter':[{'term':{'node_type':'subject'}}]}
            }
        };

        var result_keys = [
            'search_result_total', 'results', 'result_count'
        ];

        try {
            results = await osdf.query_all(es_query, namespace);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
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

    it('query_page', async function() {
        var err = null;
        var results = null;
        var page = 1;

        var es_query = {
            'query':{
                'filtered':{'filter':[{'term':{'node_type':'example'}}]}
            }
        };

        var result_keys = [
            'search_result_total', 'results', 'result_count', 'page'
        ];

        try {
            results = await osdf.query_page(es_query, namespace, page);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
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

    it('oql_query', async function() {
        var err = null;
        var results = null;
        var oql = '"example"[node_type]';

        var result_keys = [
            'search_result_total', 'results', 'result_count', 'page'
        ];

        try {
            results = await osdf.oql_query(oql, namespace);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
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

    it('oql_query_all', async function() {
        var err = null;
        var results = null;
        var oql = '"example"[node_type]';

        var result_keys = [
            'search_result_total', 'results', 'result_count'
        ];

        try {
            results = await osdf.oql_query_all(oql, namespace);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
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

    it('oql_query_page', async function() {
        var err = null;
        var results = null;

        var page = 1;
        var oql = '"example"[node_type]';
        var result_keys = [
            'search_result_total', 'results', 'result_count', 'page'
        ];

        try {
            results = await osdf.oql_query_page(oql, namespace, page);
        } catch (exception) {
            err = exception;
        }

        return new Promise(function(resolve, reject) {
            if (err) {
                assert.fail();
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

function mk_assertion_promise(func, err) {
    return new Promise(function(resolve, reject) {
        if (err) {
            assert.fail();
            reject(err);
        } else {
            func();
            resolve();
        }
    });
}

