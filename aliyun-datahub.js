
module.exports = function (RED) {
    const Datahub = require("aliyun-datahub-sdk-node");
    const { DatahubOptions, RecordSchema, TupleRecord, FieldType, CursorType } = Datahub
    // console.log(RED.nodes.registerType.toString());

    function AliyunDatahubServerNode(n) {
        console.log({ n });

        RED.nodes.createNode(this, n);

        this.name = n.name
        this.datahub = new Datahub(new DatahubOptions(n.endpoint, this.credentials.key, this.credentials.secret), JSON.parse(n.axiosOptions));
    }

    function AliyunDatahubInNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        // console.debug({ config });

        let { datahub } = RED.nodes.getNode(config.server);

        let theTopic

        datahub.getTopic(config.project, config.topic)
            .then(t => {
                theTopic = t.data
                theTopic.project = config.project
                theTopic.topic = config.topic
                // console.log({ theTopic });
            })
            .catch(err => node.error(err))

        this.on('input', async function (msg, send, done) {

            console.debug(msg.action, msg.payload);

            let _project = msg.project || config.project;
            let _topic = msg.topic || config.topic;
            let _params = msg.params || []
            let _shardId = msg.shardId || '0'
            let _schema = msg.schema || theTopic && theTopic.RecordSchema || {}
            let _action = msg.action || config.action
            let _res

            try {
                node.status({ fill: "yellow", shape: "ring", text: "start " + _action });

                console.log({ _action });

                switch (_action) {
                    case "listTopics":
                        _res = await datahub.listTopics(_project)
                        break;

                    case "getTopic":
                        _res = await datahub.getTopic(_project, _topic)
                        theTopic = _res.data
                        break;

                    case "getCursor":
                        let _cursorType = msg.cursorType.toUpperCase();
                        let _sequenceOrSystemTime = msg[_cursorType.toLowerCase().replace(/\_(\w)/g, (a, l) => l.toUpperCase())]
                        console.log({ _cursorType, _sequenceOrSystemTime });
                        _res = await datahub.getCursor(_project, _topic, CursorType[_cursorType], _shardId, _sequenceOrSystemTime)
                        break;

                    case "pull":
                        let _cursor = msg.cursor || ''
                        let _limit = msg.limit || 20
                        console.log({ _schema, _cursor, _shardId, _limit });
                        _res = await datahub.pull(_project, _topic, _schema, _cursor, _shardId, _limit)
                        break;

                    case "push":
                        let _attributes = msg.attributes || {}
                        let _payload = msg.payload.map(d => new TupleRecord(d, _schema, _attributes, _shardId))
                        console.log({ _payload });

                        _res = await datahub.push(_project, _topic, _payload)
                        break;

                    default:
                        _res = await datahub[_action](_project, _topic, ..._params)

                }

                console.log({ _res });
                node.status({ fill: "green", shape: "ring", text: _res.statusText });
                msg.payload = _res.data
                msg.action = _action
                msg.project = _project
                msg.topic = _topic
                send(msg);


            } catch (error) {
                node.status({ fill: "red", shape: "ring", text: error.message });
                if (done) { done(error) }
            }



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

    RED.nodes.registerType('aliyun-datahub-server', AliyunDatahubServerNode, opts)
    RED.nodes.registerType('datahub', AliyunDatahubInNode)

}