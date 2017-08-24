var v, DatamodelViz = {
    config: {
        container: document.getElementById('networkViz'),
        nodeConfigs: configData.nodes, // FIXME: using global
        relConfigs: configData.relationships,
        visOptions: {
            nodes: {
                borderWidth: 1,
                shadow: false
            },
            edges: {
                width: 1,
                shadow: false
            }
        },
        network: null
    },

    init: function() {
        v = this.config;
        this.initNetworkViz();
    },

    initNetworkViz: function() {

        var data = {
            nodes: new vis.DataSet(this.getNodesForViz()),
            edges: new vis.DataSet(this.getEdgesForViz()),
            config: {}
        };
        v.network = new vis.Network(v.container, data, v.visOptions);

    },

    updateNetworkViz: function() {
        var data = {
            nodes: new vis.DataSet(this.getNodesForViz()),
            edges: new vis.DataSet(this.getEdgesForViz()),
            config: {}
        };
        v.network.setData(data);
    },

    getNodesForViz: function() {
        var nodesArr = [];

        _.forEach(v.nodeConfigs, function(el, i) {
            var name = '';
            if (el.labels[0]) {
                name = el.labels[0];
            } else {
                name = el.filename || name;
            }

            var properties = "<ul>";
            _.forEach(el.properties, function(prop) {
                if (!prop.skip) {
                    if (prop.primaryKey) {
                        properties += "<li align='left'><strong>" + prop.neoKey + " : " + prop.dataType + (prop.isArray ? "[]" : "") + "</strong></li>";
                    } else {
                        properties += "<li align='left'>" + prop.neoKey + " : " + prop.dataType + (prop.isArray ? "[]" : "") + "</li>";
                    }
                }
            });

            properties += "</ul>";

            nodesArr.push({
                id: i,
                label: name,
                title: properties,
                table: el.table
            });
        });
        //console.log(nodesArr);
        return nodesArr;
    },

    getEdgesForViz: function() {
        var edgeArr = [];

        _.forEach(v.relConfigs, function(el, i) {
            if (el.startNode && el.endNode) {
                var from = _.findIndex(v.nodeConfigs, {table: el.startNode.table});
                var to = _.findIndex(v.nodeConfigs, {table: el.endNode.table});

                var properties = "<ul>";
                _.forEach(el.properties, function(prop) {
                    if (!prop.skip) {
                        properties += "<li align='left'>" + prop.neoKey + " : " + prop.dataType + (prop.isArray ? "[]" : "") + "</li>";
                    }
                });
                properties += "</ul>";

                var edge = {
                    from: from,
                    arrows: "to",
                    to: to,
                    title: properties,
                    label: el.name
                };
                edgeArr.push(edge);
            }
        });

        //console.log(edgeArr);
        return edgeArr;
    }
};