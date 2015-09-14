//
var _ = require('lodash');
var Q = require('q');
var promptly = require('promptly');
var sql = require('mssql');
var extractValues = require("extract-values");

//variabili localli
var $config;
var $conn;

//parsing della stringa in input
//reject solo in caso di exit
function parse(value) {
    //console.log('--> ', value);
    var defer = Q.defer();
    if (value == 'exit') {
        close();
        defer.reject('exit');
    } else {
        try {
            if (/^conn(ect)? /.test(value)) {
                close();
                connect(defer, createConfig(value));
            } else if (/^select /.test(value)) {
                query(defer, value);
            } else {
                exec(defer, value);
            }
        } catch (err) {
            printerr(err);
            defer.resolve();
        }
    }
    //
    return defer.promise;
}

//crea $config da una stringa
//TODO leggere config da file
//ex: Server=localhost;Database=TESTBUDGET4_TEST;User Id=sa;Password=sql2012;
function createConfig(value) {
    if (value == 'conn test') {
        value = 'Server=localhost;Database=TESTBUDGET4_TEST;User Id=sa;Password=sql2012;';
    }
    $config = extractValues(value, 'Server={server};Database={database};User Id={user};Password={password};');
}

function printerr(err) {
    console.log(err);
}

function connect(defer) {
    $conn = new sql.Connection($config, function(err) {
        if (err) {
            printerr(err);
        } else {
            print('connected to', $config.database);
        }
        defer.resolve();
    });
}

function close() {
    if ($conn) {
        $conn.close();
        $conn = null;
        print('connection closed');
    }
}

function query(defer, cmd) {
    if (!$conn) {
        printerr('ERR: no connection');
    } else {
        var req = new sql.Request($conn);
        req.query(cmd, function(err, recordset) {
            if (err) {
                printerr(err);
            } else {
                print('num rows:', recordset.length);
                for (var ii=0; ii<recordset.length; ii++) {
                    print(_.values(recordset[ii]));
                }
            }
            defer.resolve();
        });
    }
}

function exec(defer, cmd) {
    if (!$conn) {
        printerr('ERR: no connection');
    } else {
        //TODO
        defer.resolve();
    }
}

function print() {
    console.log.apply(console, _.values(arguments));
}

exports.run = function() {
    console.log('npm-mssql-query by narsenico');

    var qprompt = Q.nfbind(promptly.prompt);
    //mail loop
    var run = function() {
        setTimeout(function() {
            qprompt('sql> ')
                .then(function(value) {
                    parse(value).then(run);
                })
                .fail(printerr);
        }, 1);
    };
    run();
};
