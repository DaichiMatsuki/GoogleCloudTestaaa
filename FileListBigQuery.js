var Log4js = require('log4js');
var gcloud = require('gcloud');
var fs = require('fs');

Log4js.configure('log-config.json')
var logger = Log4js.getLogger('system');

bigquery = gcloud.bigquery({
    projectId: "newcreateproject-1085",
    keyFilename: "keyfile.json"
});

logger.info('<------------------ 処理開始 ------------------>');

var path = process.argv[2];
if (!path) {
    logger.info('ファイルパスが設定されていません');
    logger.info('<------------------ 処理エラー終了 ------------------>');
    return;
}

logger.info('初期DB処理開始 -->');

var schema = {
    "schema": {
        "fields": [
            {
                "name": "ID",
                "type": "INTEGER"
            },
            {
                "name": "Sequence",
                "type": "INTEGER"
            },
            {
                "name": "Data",
                "type": "STRING"
            }
        ]
    }
};

DeleteTable('File', 'Data').then(function () {
    
    logger.error('テーブルを削除しました');
    
    return CreateTable('File', 'Data', schema);

}).catch(function (err) {
    
    logger.error('テーブルの削除に失敗しました[err:' + err + ']');
    
    return CreateTable('File', 'Data', schema);

}).then(function (table) {
    
    logger.error('テーブルを作成しました[table:' + JSON.stringify(table) + ']');
    logger.info('<-- 初期DB処理終了');
    
    fs.open(path, 'r', function (status, fd) {
        
        if (status) {
            
            logger.error('ファイルオープンに失敗しました[path:' + path + '][status:' + status + ']');
            logger.error('<------------------ 処理エラー終了 ------------------>');
            
            return;
        }
        
        logger.info('データ登録開始 -->');
        logger.info('ファイルのオープン成功[path:' + path + ']');
        
        var rows = new Array();
        var sequence = 1;
        var readableStream = fs.createReadStream(path, { highWaterMark: 1 });
        readableStream.on('data', function (data) {
            
            rows.push({ "ID" : 1, "Sequence" : sequence++, "Data" : data.toString("hex") });

        });
        readableStream.on('end', function () {
            
            Insert('File', 'Data', rows).then(function (result) {
                
                logger.info('<-- データ登録終了');
                logger.info('登録されたデータからファイルを生成開始 -->');
                
                Query('SELECT GROUP_CONCAT(Data, "") AS File FROM ( SELECT * FROM [File.Data] ORDER BY Sequence) GROUP BY ID').then(function (result) {
                    console.log(result);
                    var copyPath = path + '-' + getCurrentTime();
                    fs.writeFile(copyPath, new Buffer(result[0].File, 'hex'), function (err) {
                        if (err) {
                            logger.error('ファイルの生成エラー[err:' + err + ']');
                            logger.info('<-- 登録されたデータからファイルを生成エラー終了');
                            logger.error('<------------------ 処理エラー終了 ------------------>');
                            return;
                        }
                        
                        logger.info('ファイルを生成[path:' + copyPath + ']');
                        logger.info('<-- 登録されたデータからファイルを生成終了');
                        logger.info('<------------------ 処理終了 ------------------>');
                        
                        return;
                    });
                });
                    
            }).catch(function (err) {
                
                logger.error('<-- データ登録エラー終了');
                logger.error('<------------------ 処理エラー終了 ------------------>');
                    
            });
        });
    });
    
    return;

}).catch(function (err) {
    
    logger.error('テーブルの作成に失敗しました[err:' + err + ']');
    logger.error('<-- 初期DB処理エラー終了');
    logger.error('<------------------ 処理エラー終了 ------------------>');

});

/* ----------------------- 定義 ----------------------- */

function CreateTable(datasetName, tableName, schema) {
    return new Promise(function (resolve, reject) {
        bigquery.dataset(datasetName).createTable(tableName, schema, function (err, table, apiResponse) {
            if (err) {
                logger.error('テーブル作成失敗[dataset:' + datasetName + '][table:' + tableName + '][schema:' + schema + ']');
                reject(err);
                return;
            }
            
            resolve(table);
        })
    });
}

function DeleteTable(datasetName, tableName) {
    return new Promise(function (resolve, reject) {
        bigquery.dataset(datasetName).table(tableName).delete(function (err, apiResponse) {
            if (err) {
                logger.error('テーブル削除失敗[dataset:' + datasetName + '][table:' + tableName + ']');
                reject(err);
                return;
            }
            
            resolve();
        });
    });
}

function Insert(datasetName, tableName, rows) {
    return new Promise(function (resolve, reject) {
        bigquery.dataset(datasetName).table(tableName).insert(rows, function (err, insertErrors, apiResponse) {
            if (err) {
                logger.error('インサート失敗[err:' + err + '][insertErrors:' + JSON.stringify(insertErrors) + ']');
                reject(err);
                return;
            }
            
            resolve();
        });
    });
}

function Query(query) {
    return new Promise(function (resolve, reject) {
        bigquery.query(query, function (err, rows, nextQuery) {
            if (err) {
                logger.error('クエリ失敗[query:' + query + '][err:' + err + ']');
                reject(err);
                return;
            }
            
            resolve(rows);
        });
    });
}

function getCurrentTime() {
    require('date-utils');
    var dt = new Date();
    return dt.toFormat("YYYYMMDDHH24MISS");
}