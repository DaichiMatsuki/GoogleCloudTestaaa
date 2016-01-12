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

var table1Name = "Data" + getCurrentTime();

CreateTable('File', table1Name, schema).then(function (table) {
    
    /* logger.error('テーブルを作成しました[table:' + JSON.stringify(table) + ']'); */
    logger.info('テーブルを作成しました');
    logger.info('<-- 初期DB処理終了');
    
    fs.open(path, 'r', function (status, fd) {
        
        if (status) {
            
            logger.error('ファイルオープンに失敗しました[path:' + path + '][status:' + status + ']');
            logger.error('<------------------ 処理エラー終了 ------------------>');
            
            return;
        }
        
        logger.info('データ登録用CSV作成開始 -->');
        logger.info('ファイルのオープン成功[path:' + path + ']');
        
        var csvPath = getCurrentTime() + ".csv";
        fs.writeFileSync(csvPath, "ID,Sequence,Data\n", 'utf8');
        
        var sequence = 1;
        var readableStream = fs.createReadStream(path, { highWaterMark: 1 });
        readableStream.on('data', function (data) {
            
            fs.appendFileSync(csvPath, '1,' + sequence++ + ',"' + data.toString("hex") + '"\n', 'utf8');

        });
        readableStream.on('end', function () {
            
            logger.info('<-- データ登録用CSV作成終了');
            logger.info('データ登録開始 -->');
            
            var metadata = {
                allowJaggedRows: true,
                skipLeadingRows: 1
            };
            
            fs.createReadStream(csvPath).pipe(bigquery.dataset('File').table(table1Name).createWriteStream(metadata)).on('complete', function (job) {
                
                Promise.resolve(0).then(function loop(i) {
                    return GetJob(job.id).then(function (table) {
                        
                        logger.info('<-- データ登録終了');
                        logger.info('登録されたデータからファイルを生成開始 -->');
                        
                        Query('SELECT GROUP_CONCAT(Data, "") AS File FROM ( SELECT * FROM [File.' + table1Name + '] ORDER BY Sequence) GROUP BY ID').then(function (result) {
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
                    
                    
                    }).catch(loop);
                });
                
                
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

function GetJob(jobID) {
    return new Promise(function (resolve, reject) {
        bigquery.job(jobID).getMetadata(function (err, job, apiResponse) {
            logger.info(job.status.state);
            if (job.status.state == 'DONE')
                resolve(job);
            
            Sleep(1);
            reject(job);
        })
    });
}

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
        bigquery.query(query, function (err, rows) {
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

function Sleep(T) {
    var d1 = new Date().getTime();
    var d2 = new Date().getTime();
    while (d2 < d1 + 1000 * T) {    //T秒待つ 
        d2 = new Date().getTime();
    }
    return;
} 