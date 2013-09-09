/*global Backbone */
var app = app || {};

(function () {
    'use strict';

    var MonitorRouter = Backbone.Router.extend({
        routes: {
            '*filter': 'setFilter'
        },

        setFilter: function (param) {
            app.MonitorFilter = param || '';
            app.monitor.trigger('filter');
        }
    });

    app.MonitorRouter = new MonitorRouter();
    Backbone.history.start();
})();
