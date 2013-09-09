/*global Backbone */
var app = app || {};

(function () {
        'use strict';

    var HostMetrics = Backbone.Collection.extend({
        model: app.HostMetric,
        url_api: 'http://172.17.3.188:4321/monitorapi/host/',
    });
    app.hostmetrics = new HostMetrics();
})();
