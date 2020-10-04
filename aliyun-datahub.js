
module.exports = function (RED) {
    const Datahub = require("aliyun-datahub-sdk-node");
    const { DatahubOptions, RecordSchema, TupleRecord, FieldType } = Datahub
    console.log(RED.nodes.registerType.toString());

    function AliyunDatahubEndpointNode(n) {
        RED.nodes.createNode(this, n);

        this.name = n.name
        this.datahub = new Datahub(new DatahubOptions(n.endpoint, this.credentials.key, this.credentials.secret));
    }

    function AliyunDatahubInNode(config) {
        RED.nodes.createNode(this, config);
        // let node = this;

        console.log({ config });

        let { datahub } = RED.nodes.getNode(config.server);

        this.on('input', async function (msg, send, done) {

            const testRecordSchema = new RecordSchema([
                //[ name, type, notnull ]   
                ['name', FieldType.STRING, true]
            ])

            try {
                let _cursorRes = await datahub.getCursor( config.project, msg.topic)
                console.log({ _cursorRes });
                let _cursor = msg.cursor // _cursorRes.data.Cursor
    
                let _pullRes = await datahub.pull( config.project, msg.topic, testRecordSchema, _cursor, '0', 100)
                console.log({_pullRes});
                msg.payload = _pullRes.data
                send(msg);
            } catch (error) {
                if (done) { done(error) }
            }


            // this.status({ fill: "red", shape: "ring", text: "disconnected" });

        });

        this.on('close', function (done) {
            done();
        });
    }

    function AliyunDatahubOutNode(config) {
        RED.nodes.createNode(this, config);
        // let node = this;

        console.log({ config });

        let { datahub } = RED.nodes.getNode(config.server);

        this.on('input', async function (msg, send, done) {

            const testRecordSchema = new RecordSchema([
                //[ name, type, notnull ]   
                ['name', FieldType.STRING, true]
            ])
            // // 推送数据
            // let testData = {
            //     name: 'abc'
            // }

            let testTupleRecords = []
            testTupleRecords.push(new TupleRecord(msg.payload, testRecordSchema))

            try {
                let _pushRes = (await datahub.push(config.project, msg.topic, testTupleRecords))
                console.log({ _pushRes });
                msg.payload = _pushRes.data
                send(msg)
            } catch (err) {
                if (done) { done(err) }
            }

            // this.status({ fill: "red", shape: "ring", text: "disconnected" });

        });

        this.on('close', function (done) {
            done();
        });
    }

    let opts = {
        credentials: {
            key: { type: "text" }, secret: { type: "password" },
        },
    }

    RED.nodes.registerType('aliyun-datahub', AliyunDatahubEndpointNode, opts)
    RED.nodes.registerType('aliyun-datahub in', AliyunDatahubInNode)
    RED.nodes.registerType('aliyun-datahub out', AliyunDatahubOutNode)

}