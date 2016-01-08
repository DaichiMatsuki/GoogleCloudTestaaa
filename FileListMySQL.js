var Log4js = require('log4js');
var mysql = require('mysql');
var fs = require('fs');

Log4js.configure('log-config.json');
var logger = Log4js.getLogger('system');

logger.info('<------------------ 処理開始 ------------------>');

var path = process.argv[2];
if (!path) {
    logger.info('ファイルパスが設定されていません');
    logger.info('<------------------ 処理エラー終了 ------------------>');
    return;
}

var connection = mysql.createConnection({
    host     : '104.199.148.74',
    user     : 'root',
    password : 'PToq86idm',
    database : 'File'
});

Connect().then(function () {
    
    logger.info('初期DB処理開始 -->');
    
    var tasks = [
        /* Query('DELETE FROM List'), */
        Query('DELETE FROM Data')
    ];
    Promise.all(tasks).then(function (results) {
        
        logger.info('<-- 初期DB処理終了');
        logger.info('データ登録開始 -->');
        
        fs.open(path, 'r', function (status, fd) {
            
            if (status) {
                logger.error('ファイルオープンに失敗しました[path:' + path + '][status:' + status + ']');
                logger.error('<-- データ登録エラー終了');
                logger.error('<------------------ 処理エラー終了 ------------------>');
                return;
            }
            
            logger.info('ファイルのオープン成功[path:' + path + ']');
            
            var sequence = 1;
            var readableStream = fs.createReadStream(path, { highWaterMark: 1 });
            readableStream.on('data', function (data) {
                
                Query('INSERT INTO File.Data VALUES ( 1, ' + sequence++ + ', "' + data.toString("hex") + '"  )').then(function (result) {
                    
                }).catch(function (err) {
                    
                });
            });
            readableStream.on('end', function () {
                
                logger.info('<-- データ登録終了');
                logger.info('登録されたデータからファイルを生成開始 -->');
                
                Query('SELECT GROUP_CONCAT(Data SEPARATOR "") AS File FROM ( SELECT * FROM File.Data ORDER BY Sequence) GROUP BY ID').then(function (result) {
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
                
                return;
            });
            
            /*
            
            Query('INSERT INTO File.List VALUES ( 1, "2016/01/05 00:00:00", null, false, "' + path + '", 0, "aaa"  )').then(function () {
                
                logger.info('List登録完了');
                
                var readableStream = fs.createReadStream(path, { highWaterMark: 1 });
                readableStream.on('data', function (data) {
                    console.log(data + '/');
                });
                readableStream.on('end', function () {
                    
                    logger.info('<-- データ登録終了');
                    logger.info('<------------------ 処理終了 ------------------>');
                    
                    return;
                });
                                                                                                                                                                                                                                                                                             
            }).catch(function (err) {
                
                logger.error('List登録失敗');
                logger.error('<-- データ登録エラー終了');
                logger.error('<------------------ 処理エ ------------------>');
                
                return;
            });
                                                                                                                                                                                                                                                                                                                                                                                                                                 
            /*  */

        });
        
        return;

    }).catch(function (err) {
        
        logger.error('<-- 初期DB処理エラー終了');
        logger.error('<------------------ 処理エラー終了 ------------------>');
        
        return;
    });
});

/*
 * 'CREATE DATABASE IF NOT EXISTS File'
 * 'Use File'
 * 'CREATE TABLE IF NOT EXISTS List ( ID int not null, CreateTime timestamp not null, UpdateTime timestamp, DeleteFlag boolean not null, FileName text, FileSize int, FileType text )'
 * 'CREATE TABLE IF NOT EXISTS Data ( ID int not null, Sequence int not null, Data text ) '
 */

/* ----------------------- 定義 ----------------------- */

function Connect() {
    return new Promise(function (resolve, reject) {
        connection.connect(function (err) {
            if (err) {
                logger.error('MySQL接続失敗[err:' + err + ']');
                return;
            }
            
            logger.info('MySQL接続成功');
            resolve();
        });
    });
}

function Query(query) {
    return new Promise(function (resolve, reject) {
        connection.query(query, function (err, result) {
            if (err) {
                logger.error('クエリ失敗[query:' + query + '][err:' + err + ']');
                reject(err);
                return;
            }
            
            /* logger.info('クエリ成功[query:' + query + ']'); */
            resolve(result);
        });
    });
}

function getCurrentTime() {
    require('date-utils');
    var dt = new Date();
    return dt.toFormat("YYYYMMDDHH24MISS");
}