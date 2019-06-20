const amqp = require('amqplib');
const request = require('request');
const EventEmitter = require('events');
const bunyan = require('bunyan');

// if the connection is closed or fails to be established at all, we will reconnect
let mqConn = {};
let healthInterval = null;
let disconneted = false;
let mqConfig;

class HealthFailur extends EventEmitter {}
const healthFailur = new HealthFailur();

const log = bunyan.createLogger({
    name: "coned_autoenroll_batchIntake_consumer_connect",
    streams: [{
        type: 'rotating-file',
        path: process.env.CONED_AUTOENROLL_CONSUMER_LOGPATH_CONNECT,
        period: '10d',
        count: 4
    }]
});

async function connectAsync(config) {
    return new Promise(async (resolve, reject)=>{

        // asseration on configuration
        if (!config){
           return reject('Config file required');
        }

        //catching the configuration
        mqConfig = config;
        if (mqConn.connection) {
            return resolve(mqConn);
        }

        console.log("[AMQP] Creating a connection");
        try {
            mqConn = await amqp.connect(mqConfig.MQServer);
            mqConn.on("error", (err) => {
                if (err.message !== "Connection closing") {
                    console.error("[AMQP] Connection Error");
                    log.error("[AMQP] conn error", err.message);
                    disconneted = true;
                }
            });
            mqConn.on("close", function () {
                console.warn("[AMQP] Connection Close");
                disconneted = true;
            });

            log.error("[AMQP] connected");
            startHealthCheck();

            return resolve(mqConn);
        } catch (err) {
            console.log(err);
        }
    });
}

const startHealthCheck = () => {
    if (healthInterval !== null) {
        try {
            clearInterval(healthInterval);
        } catch (err) {
            console.warn('[AMQP] Health check Faild')
            log.error(err);
        }
    }
    healthInterval = setInterval(healthCheck, mqConfig.MQURL.interval);
}

const handleDisconnect = () => {
    if (disconneted) {
        console.log('[AMQP] retry connection');
        disconneted = false;
        healthFailur.emit('retry');
    }
}

const reconnect = (channel)=>{
    console.log('[AMQP] reconnecting');
    if (channel){
        channel.close();
        if (mqConn.close){
            mqConn.close();
            mqConn = {};
            setTimeout(()=>healthFailur.emit('connect'), 2000)
        }
    }
}

const healthCheck = async () => {
    let url = mqConfig.MQURL.url;
    request(url, {
        'auth': {
            'user': mqConfig.MQServer.username,
            'pass': mqConfig.MQServer.password,
            'sendImmediately': false
        }
    }, (err, response, body) => {
        if (err || !body || JSON.parse(body).status !== 'ok') {
            log.error(err);
            disconneted = true;
        } else {
            handleDisconnect();
        }
    });
}

module.exports = {
    connectAsync,
    healthFailur,
    reconnect
};